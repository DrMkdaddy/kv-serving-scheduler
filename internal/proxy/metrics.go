package proxy

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type tenantMetrics struct {
	Requests  int64     `json:"requests"`
	Completed int64     `json:"completed"`
	Failed    int64     `json:"failed"`
	Preempted int64     `json:"preempted"`
	Tokens    int64     `json:"tokens"`
	TTFT      []float64 `json:"-"`
	QueueWait []float64 `json:"-"`
}

// Metrics is the real proxy's observable state. It is intentionally small:
// everything here is either a counter, a gauge, or a bounded sample of
// latencies used for p50/p99.
type Metrics struct {
	mu        sync.Mutex
	start     time.Time
	requests  int64
	completed int64
	failed    int64
	preempted int64
	admitted  int64
	tokens    int64
	ttft      []float64
	itl       []float64
	queueWait []float64
	tenants   map[string]*tenantMetrics
}

func NewMetrics() *Metrics {
	return &Metrics{start: time.Now(), tenants: make(map[string]*tenantMetrics)}
}

func (m *Metrics) tenant(name string) *tenantMetrics {
	t, ok := m.tenants[name]
	if !ok {
		t = &tenantMetrics{}
		m.tenants[name] = t
	}
	return t
}

func appendBounded(slice []float64, value float64, limit int) []float64 {
	if len(slice) >= limit {
		copy(slice, slice[1:])
		slice[len(slice)-1] = value
		return slice
	}
	return append(slice, value)
}

func (m *Metrics) Request(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests++
	m.tenant(tenant).Requests++
}

func (m *Metrics) Admit(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.admitted++
}

func (m *Metrics) Preempt(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preempted++
	m.tenant(tenant).Preempted++
}

func (m *Metrics) Fail(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
	m.tenant(tenant).Failed++
}

func (m *Metrics) Finish(tenant string, tokens int, ttftMs, queueMs float64, itls []float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed++
	m.tokens += int64(tokens)
	m.ttft = appendBounded(m.ttft, ttftMs, 4096)
	m.queueWait = appendBounded(m.queueWait, queueMs, 4096)
	for _, itl := range itls {
		m.itl = appendBounded(m.itl, itl, 8192)
	}
	t := m.tenant(tenant)
	t.Completed++
	t.Tokens += int64(tokens)
	t.TTFT = appendBounded(t.TTFT, ttftMs, 1024)
	t.QueueWait = appendBounded(t.QueueWait, queueMs, 1024)
}

func quantile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

type Stats struct {
	UptimeSeconds float64               `json:"uptime_seconds"`
	Requests      int64                 `json:"requests"`
	Admitted      int64                 `json:"admitted"`
	Completed     int64                 `json:"completed"`
	Failed        int64                 `json:"failed"`
	Preempted     int64                 `json:"preempted"`
	Tokens        int64                 `json:"tokens"`
	TTFTP50Ms     float64               `json:"ttft_p50_ms"`
	TTFTP95Ms     float64               `json:"ttft_p95_ms"`
	TTFTP99Ms     float64               `json:"ttft_p99_ms"`
	ITLP50Ms      float64               `json:"itl_p50_ms"`
	ITLP95Ms      float64               `json:"itl_p95_ms"`
	ITLP99Ms      float64               `json:"itl_p99_ms"`
	QueueP50Ms    float64               `json:"queue_p50_ms"`
	QueueP99Ms    float64               `json:"queue_p99_ms"`
	Tenants       map[string]TenantView `json:"tenants"`
}

type TenantView struct {
	Requests   int64   `json:"requests"`
	Completed  int64   `json:"completed"`
	Failed     int64   `json:"failed"`
	Preempted  int64   `json:"preempted"`
	Tokens     int64   `json:"tokens"`
	TTFTP99Ms  float64 `json:"ttft_p99_ms"`
	QueueP99Ms float64 `json:"queue_p99_ms"`
}

func (m *Metrics) Snapshot() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := Stats{
		UptimeSeconds: time.Since(m.start).Seconds(),
		Requests:      m.requests,
		Admitted:      m.admitted,
		Completed:     m.completed,
		Failed:        m.failed,
		Preempted:     m.preempted,
		Tokens:        m.tokens,
		TTFTP50Ms:     quantile(m.ttft, 0.50),
		TTFTP95Ms:     quantile(m.ttft, 0.95),
		TTFTP99Ms:     quantile(m.ttft, 0.99),
		ITLP50Ms:      quantile(m.itl, 0.50),
		ITLP95Ms:      quantile(m.itl, 0.95),
		ITLP99Ms:      quantile(m.itl, 0.99),
		QueueP50Ms:    quantile(m.queueWait, 0.50),
		QueueP99Ms:    quantile(m.queueWait, 0.99),
		Tenants:       make(map[string]TenantView, len(m.tenants)),
	}
	for name, t := range m.tenants {
		stats.Tenants[name] = TenantView{
			Requests: t.Requests, Completed: t.Completed, Failed: t.Failed,
			Preempted: t.Preempted, Tokens: t.Tokens,
			TTFTP99Ms: quantile(t.TTFT, 0.99), QueueP99Ms: quantile(t.QueueWait, 0.99),
		}
	}
	return stats
}

func (m *Metrics) Prometheus() string {
	s := m.Snapshot()
	var b strings.Builder
	write := func(name, help, typ string, value any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, value)
	}
	write("scheduler_requests_total", "Requests accepted by the proxy", "counter", s.Requests)
	write("scheduler_admitted_total", "Requests dispatched to a slot", "counter", s.Admitted)
	write("scheduler_completed_total", "Requests completed", "counter", s.Completed)
	write("scheduler_failed_total", "Requests failed or rejected", "counter", s.Failed)
	write("scheduler_preemptions_total", "Running requests preempted for KV budget", "counter", s.Preempted)
	write("scheduler_tokens_total", "Generated tokens", "counter", s.Tokens)
	write("scheduler_ttft_p50_ms", "Time to first token, p50", "gauge", s.TTFTP50Ms)
	write("scheduler_ttft_p99_ms", "Time to first token, p99", "gauge", s.TTFTP99Ms)
	write("scheduler_itl_p50_ms", "Inter-token latency, p50", "gauge", s.ITLP50Ms)
	write("scheduler_itl_p99_ms", "Inter-token latency, p99", "gauge", s.ITLP99Ms)
	write("scheduler_queue_p99_ms", "Queue wait, p99", "gauge", s.QueueP99Ms)
	return b.String()
}
