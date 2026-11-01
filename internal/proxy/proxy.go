// Package proxy is the real scheduler in front of llama-server.
//
// It reserves the worst-case KV footprint at admission, assigns each request a
// fixed upstream slot, streams the upstream response internally to measure
// TTFT and ITL, and preempts a running request by cancelling it and requeueing
// it for recomputation when a higher-priority request cannot fit.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DrMkdaddy/kv-serving-scheduler/internal/kv"
	"github.com/DrMkdaddy/kv-serving-scheduler/internal/sched"
)

type Config struct {
	Listen          string                `json:"listen"`
	Upstream        string                `json:"upstream"`
	Scheduler       sched.SchedulerConfig `json:"scheduler"`
	MaxRequestBytes int64                 `json:"max_request_bytes"`
	Timeout         time.Duration         `json:"-"`
	TimeoutMs       int                   `json:"timeout_ms"`
}

type outcome struct {
	status  int
	body    []byte
	headers map[string]string
}

type pending struct {
	id           string
	tenant       string
	body         []byte
	path         string
	promptTokens int
	maxTokens    int
	reserved     int
	priority     int
	weight       float64
	arrival      time.Time
	admitted     time.Time
	virtualEnd   float64
	ctx          context.Context
	cancel       context.CancelFunc
	result       chan outcome

	preempted   atomic.Bool
	done        atomic.Bool
	attempt     uint64
	runKey      string
	generated   int
	firstToken  time.Time
	lastToken   time.Time
	itls        []float64
	preemptions int
	slot        int
}

type Server struct {
	cfg      Config
	client   *http.Client
	alloc    *kv.Allocator
	metrics  *Metrics
	upstream *url.URL

	mu        sync.Mutex
	queue     []*pending
	running   map[string]*pending
	freeSlots []int
	tenantVT  map[string]float64
	stop      chan struct{}
	closeOnce sync.Once
	nextID    atomic.Uint64
}

func New(cfg Config) (*Server, error) {
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:8080"
	}
	if cfg.Upstream == "" {
		return nil, fmt.Errorf("upstream is required")
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = 4 << 20
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 300000
	}
	cfg.Timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	upstream, err := url.Parse(cfg.Upstream)
	if err != nil {
		return nil, err
	}
	alloc, err := kv.New(cfg.Scheduler.BlockSize, cfg.Scheduler.TotalBlocks)
	if err != nil {
		return nil, err
	}
	free := make([]int, cfg.Scheduler.Slots)
	for i := range free {
		free[i] = i
	}
	s := &Server{
		cfg:       cfg,
		client:    &http.Client{Timeout: cfg.Timeout},
		alloc:     alloc,
		metrics:   NewMetrics(),
		upstream:  upstream,
		running:   make(map[string]*pending),
		freeSlots: free,
		tenantVT:  make(map[string]float64),
		stop:      make(chan struct{}),
	}
	go s.loop()
	return s, nil
}

func (s *Server) Close() { s.closeOnce.Do(func() { close(s.stop) }) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/completion", s.handleRequest)
	mux.HandleFunc("/v1/chat/completions", s.handleRequest)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	target := *s.upstream
	target.Path = "/health"
	resp, err := s.client.Get(target.String())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	queue := len(s.queue)
	running := len(s.running)
	freeSlots := len(s.freeSlots)
	st := s.alloc.Stats()
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, s.metrics.Prometheus())
	fmt.Fprintf(w, "# HELP scheduler_queue_depth Requests waiting for a slot or KV blocks\n")
	fmt.Fprintf(w, "# TYPE scheduler_queue_depth gauge\nscheduler_queue_depth %d\n", queue)
	fmt.Fprintf(w, "# TYPE scheduler_active_requests gauge\nscheduler_active_requests %d\n", running)
	fmt.Fprintf(w, "# TYPE scheduler_free_slots gauge\nscheduler_free_slots %d\n", freeSlots)
	fmt.Fprintf(w, "# TYPE scheduler_kv_blocks_used gauge\nscheduler_kv_blocks_used %d\n", st.UsedBlocks)
	fmt.Fprintf(w, "# TYPE scheduler_kv_blocks_free gauge\nscheduler_kv_blocks_free %d\n", st.FreeBlocks)
	fmt.Fprintf(w, "# TYPE scheduler_kv_waste_tokens gauge\nscheduler_kv_waste_tokens %d\n", st.InternalWasteTokens)
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	queue := len(s.queue)
	running := len(s.running)
	freeSlots := len(s.freeSlots)
	st := s.alloc.Stats()
	s.mu.Unlock()
	payload := map[string]any{
		"metrics":    s.metrics.Snapshot(),
		"queue":      queue,
		"running":    running,
		"free_slots": freeSlots,
		"kv":         st,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxRequestBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if stream, ok := req["stream"].(bool); ok && stream {
		http.Error(w, "this proxy measures streaming upstream but returns a non-streaming response", http.StatusNotImplemented)
		return
	}
	prompt, maxTokens, tenant, priority, err := extractRequest(r.URL.Path, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	p := &pending{
		id:        fmt.Sprintf("p%06d", s.nextID.Add(1)),
		tenant:    tenant,
		body:      body,
		path:      r.URL.Path,
		maxTokens: maxTokens,
		priority:  priority,
		weight:    1,
		arrival:   time.Now(),
		ctx:       ctx,
		cancel:    cancel,
		result:    make(chan outcome, 1),
	}
	if v, ok := req["weight"].(float64); ok && v > 0 {
		p.weight = v
	}
	p.promptTokens = s.countTokens(prompt)
	if p.promptTokens < 1 {
		p.promptTokens = 1
	}
	p.reserved = p.promptTokens + p.maxTokens
	if p.reserved > s.cfg.Scheduler.TotalBlocks*s.cfg.Scheduler.BlockSize {
		cancel()
		s.metrics.Request(p.tenant)
		s.metrics.Fail(p.tenant)
		http.Error(w, "request exceeds the KV budget", http.StatusRequestEntityTooLarge)
		return
	}
	s.metrics.Request(p.tenant)

	s.mu.Lock()
	if len(s.queue) >= s.cfg.Scheduler.MaxQueue {
		s.mu.Unlock()
		cancel()
		s.metrics.Fail(p.tenant)
		http.Error(w, "queue full", http.StatusTooManyRequests)
		return
	}
	s.setVirtualEndLocked(p)
	s.queue = append(s.queue, p)
	s.mu.Unlock()

	out := <-p.result
	for k, v := range out.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(out.status)
	w.Write(out.body)
}

func extractRequest(path string, req map[string]any) (prompt string, maxTokens int, tenant string, priority int, err error) {
	maxTokens = 128
	tenant = "default"
	if v, ok := req["tenant"].(string); ok && v != "" {
		tenant = v
	}
	if v, ok := req["priority"].(float64); ok {
		priority = int(v)
	}
	if path == "/completion" {
		p, ok := req["prompt"].(string)
		if !ok {
			return "", 0, "", 0, fmt.Errorf("prompt must be a string")
		}
		prompt = p
		if v, ok := req["n_predict"].(float64); ok && v > 0 {
			maxTokens = int(v)
		}
		return prompt, maxTokens, tenant, priority, nil
	}
	msgs, ok := req["messages"].([]any)
	if !ok {
		return "", 0, "", 0, fmt.Errorf("messages must be an array")
	}
	var b strings.Builder
	for _, raw := range msgs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		content, _ := m["content"].(string)
		b.WriteString(role)
		b.WriteString(": ")
		b.WriteString(content)
		b.WriteString("\n")
	}
	prompt = b.String()
	if v, ok := req["max_tokens"].(float64); ok && v > 0 {
		maxTokens = int(v)
	}
	// Chat templates add control tokens; this is an admission estimate, not a
	// tokenizer replacement. The sentinel keeps the reservation conservative.
	return prompt, maxTokens + 16, tenant, priority, nil
}

func (s *Server) countTokens(text string) int {
	body, _ := json.Marshal(map[string]any{"content": text})
	target := *s.upstream
	target.Path = "/tokenize"
	resp, err := s.client.Post(target.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return len(text)/4 + 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return len(text)/4 + 1
	}
	var payload struct {
		Tokens []int `json:"tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil || len(payload.Tokens) == 0 {
		return len(text)/4 + 1
	}
	return len(payload.Tokens)
}

func (s *Server) setVirtualEndLocked(p *pending) {
	if s.cfg.Scheduler.Policy != "wfq" {
		p.virtualEnd = float64(p.arrival.UnixNano()) / 1e6
		return
	}
	start := float64(p.arrival.UnixNano()) / 1e6
	if s.tenantVT[p.tenant] > start {
		start = s.tenantVT[p.tenant]
	}
	cost := float64(p.reserved)
	weight := p.weight
	if weight <= 0 {
		weight = 1
	}
	p.virtualEnd = start + cost/weight
	s.tenantVT[p.tenant] = p.virtualEnd
}

func (s *Server) less(a, b *pending) bool {
	switch s.cfg.Scheduler.Policy {
	case "priority":
		if a.priority != b.priority {
			return a.priority < b.priority
		}
		return a.arrival.Before(b.arrival)
	case "wfq":
		return a.virtualEnd < b.virtualEnd
	default:
		return a.arrival.Before(b.arrival)
	}
}

func (s *Server) loop() {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			s.dispatchLocked()
			s.mu.Unlock()
		}
	}
}

func (s *Server) dispatchLocked() {
	for {
		if len(s.queue) == 0 {
			return
		}
		chosen := -1
		var best *pending
		for i, p := range s.queue {
			if !s.alloc.CanFit(p.reserved) {
				continue
			}
			if best == nil || s.less(p, best) {
				best = p
				chosen = i
			}
		}
		if best == nil {
			candidate := s.bestQueuedLocked()
			if candidate == nil || !s.shouldPreemptForKvLocked(candidate) {
				return
			}
			if !s.preemptLocked() {
				return
			}
			continue
		}
		if len(s.freeSlots) == 0 {
			if !s.shouldPreemptForSlotLocked(best) {
				return
			}
			if !s.preemptLocked() {
				return
			}
			// The victim's slot is still draining; wait for release.
			return
		}
		if !s.alloc.Alloc(best.id, best.promptTokens) {
			return
		}
		s.queue = append(s.queue[:chosen], s.queue[chosen+1:]...)
		slot := s.freeSlots[0]
		s.freeSlots = s.freeSlots[1:]
		best.slot = slot
		best.admitted = time.Now()
		best.attempt++
		best.runKey = fmt.Sprintf("%s#%d", best.id, best.attempt)
		best.preempted.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		best.ctx, best.cancel = ctx, cancel
		s.running[best.runKey] = best
		s.metrics.Admit(best.tenant)
		go s.run(best, best.attempt)
	}
}

func (s *Server) bestQueuedLocked() *pending {
	var best *pending
	for _, p := range s.queue {
		if best == nil || s.less(p, best) {
			best = p
		}
	}
	return best
}

func (s *Server) worstRunningLocked() *pending {
	var victim *pending
	for _, p := range s.running {
		if p.preempted.Load() || p.done.Load() {
			continue
		}
		if victim == nil {
			victim = p
			continue
		}
		switch s.cfg.Scheduler.Victim {
		case "largest":
			if p.reserved > victim.reserved {
				victim = p
			}
		case "newest":
			if p.arrival.After(victim.arrival) {
				victim = p
			}
		case "oldest":
			if p.arrival.Before(victim.arrival) {
				victim = p
			}
		default:
			if p.priority > victim.priority ||
				(p.priority == victim.priority && p.arrival.After(victim.arrival)) {
				victim = p
			}
		}
	}
	return victim
}

func (s *Server) shouldPreemptForKvLocked(candidate *pending) bool {
	if s.cfg.Scheduler.Preempt == "none" || len(s.running) == 0 {
		return false
	}
	victim := s.worstRunningLocked()
	if victim == nil {
		return false
	}
	switch s.cfg.Scheduler.Policy {
	case "priority":
		return candidate.priority < victim.priority
	case "wfq":
		return candidate.virtualEnd < victim.virtualEnd
	default:
		// FCFS does not evict a resident request for a newer arrival.
		return false
	}
}

func (s *Server) shouldPreemptForSlotLocked(candidate *pending) bool {
	if s.cfg.Scheduler.Preempt == "none" || len(s.running) == 0 {
		return false
	}
	victim := s.worstRunningLocked()
	if victim == nil {
		return false
	}
	switch s.cfg.Scheduler.Policy {
	case "priority":
		return candidate.priority < victim.priority
	case "wfq":
		return candidate.virtualEnd < victim.virtualEnd
	default:
		// FCFS does not preempt an older request for a newer one.
		return false
	}
}

func (s *Server) preemptLocked() bool {
	victim := s.worstRunningLocked()
	if victim == nil {
		return false
	}
	victim.preempted.Store(true)
	victim.cancel()
	s.alloc.Free(victim.id)
	s.metrics.Preempt(victim.tenant)
	victim.preemptions++
	victim.generated = 0
	victim.itls = nil
	victim.firstToken = time.Time{}
	s.setVirtualEndLocked(victim)
	s.queue = append(s.queue, victim)
	return true
}

func (s *Server) run(p *pending, attempt uint64) {
	defer s.release(p, attempt)
	if p.preempted.Load() || p.attempt != attempt {
		return
	}
	body, err := injectSlot(p.body, p.slot)
	if err != nil {
		s.fail(p, attempt, http.StatusInternalServerError, err)
		return
	}
	target := *s.upstream
	target.Path = p.path
	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		s.fail(p, attempt, http.StatusInternalServerError, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		if p.preempted.Load() || p.attempt != attempt {
			return
		}
		s.fail(p, attempt, http.StatusBadGateway, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if p.preempted.Load() || p.attempt != attempt {
			return
		}
		s.fail(p, attempt, resp.StatusCode, fmt.Errorf("upstream: %s", string(raw)))
		return
	}

	var content strings.Builder
	var timings map[string]any
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}
		if t, ok := event["timings"].(map[string]any); ok {
			timings = t
		}
		chunk := contentOf(event, p.path)
		if chunk == "" {
			continue
		}
		now := time.Now()
		if p.firstToken.IsZero() {
			p.firstToken = now
		} else {
			p.itls = append(p.itls, float64(now.Sub(p.lastToken))/float64(time.Millisecond))
		}
		p.lastToken = now
		p.generated++
		content.WriteString(chunk)
		s.mu.Lock()
		s.alloc.Grow(p.id, p.promptTokens+p.generated)
		s.mu.Unlock()
	}
	if err := scanner.Err(); err != nil {
		if p.preempted.Load() || p.attempt != attempt {
			return
		}
		s.fail(p, attempt, http.StatusBadGateway, err)
		return
	}
	if p.preempted.Load() || p.attempt != attempt {
		return
	}
	out := buildResponse(p, content.String(), timings)
	ttft := float64(0)
	if !p.firstToken.IsZero() {
		ttft = float64(p.firstToken.Sub(p.arrival)) / float64(time.Millisecond)
	}
	queueMs := float64(0)
	if !p.admitted.IsZero() {
		queueMs = float64(p.admitted.Sub(p.arrival)) / float64(time.Millisecond)
	}
	s.mu.Lock()
	if !p.done.Load() {
		s.alloc.Free(p.id)
		p.done.Store(true)
		s.metrics.Finish(p.tenant, p.generated, ttft, queueMs, p.itls)
	}
	s.mu.Unlock()
	p.result <- out
}

func (s *Server) fail(p *pending, attempt uint64, status int, err error) {
	if p.preempted.Load() || p.done.Load() || p.attempt != attempt {
		return
	}
	p.done.Store(true)
	s.mu.Lock()
	s.alloc.Free(p.id)
	s.mu.Unlock()
	s.metrics.Fail(p.tenant)
	body, _ := json.Marshal(map[string]string{"error": err.Error()})
	p.result <- outcome{status: status, body: body}
}

func (s *Server) release(p *pending, attempt uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[p.runKey]; ok {
		delete(s.running, p.runKey)
		s.freeSlots = append(s.freeSlots, p.slot)
	}
	if p.attempt == attempt && !p.done.Load() && !p.preempted.Load() {
		s.alloc.Free(p.id)
	}
}

func injectSlot(body []byte, slot int) ([]byte, error) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	req["id_slot"] = slot
	req["stream"] = true
	req["cache_prompt"] = false
	// Scheduler-only fields must not reach llama-server.
	delete(req, "tenant")
	delete(req, "priority")
	return json.Marshal(req)
}

func contentOf(event map[string]any, path string) string {
	if path == "/completion" {
		if s, ok := event["content"].(string); ok {
			return s
		}
		return ""
	}
	choices, _ := event["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	choice, _ := choices[0].(map[string]any)
	delta, _ := choice["delta"].(map[string]any)
	content, _ := delta["content"].(string)
	return content
}

func buildResponse(p *pending, content string, timings map[string]any) outcome {
	queueMs := "0"
	if !p.admitted.IsZero() {
		queueMs = fmt.Sprintf("%.1f", float64(p.admitted.Sub(p.arrival))/float64(time.Millisecond))
	}
	headers := map[string]string{
		"X-Scheduler-Queue-Ms":    queueMs,
		"X-Scheduler-Preemptions": strconv.Itoa(p.preemptions),
		"X-Scheduler-Slot":        strconv.Itoa(p.slot),
	}
	if !p.firstToken.IsZero() {
		ttft := float64(p.firstToken.Sub(p.arrival)) / float64(time.Millisecond)
		headers["X-Scheduler-TTFT-Ms"] = fmt.Sprintf("%.1f", ttft)
	}
	var payload any
	if p.path == "/completion" {
		payload = map[string]any{"content": content, "timings": timings, "stop": true}
	} else {
		payload = map[string]any{
			"id":      "chatcmpl-scheduler",
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"choices": []any{map[string]any{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": content},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{
				"prompt_tokens":     p.promptTokens,
				"completion_tokens": p.generated,
				"total_tokens":      p.promptTokens + p.generated,
			},
		}
	}
	body, _ := json.Marshal(payload)
	return outcome{status: 200, body: body, headers: headers}
}

// UpstreamProxy is exposed for tests that need a reverse proxy without the
// scheduler. It is not used by the server path.
func UpstreamProxy(target string) (*httputil.ReverseProxy, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(u), nil
}
