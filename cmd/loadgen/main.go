package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type record struct {
	ID          int     `json:"id"`
	Status      int     `json:"status"`
	WallMs      float64 `json:"wall_ms"`
	QueueMs     float64 `json:"queue_ms"`
	TTFTMs      float64 `json:"ttft_ms"`
	Preemptions int     `json:"preemptions"`
	Tokens      int     `json:"tokens"`
	Error       string  `json:"error,omitempty"`
}

type artifact struct {
	Schema      string         `json:"schema"`
	GeneratedAt string         `json:"generated_at"`
	URL         string         `json:"url"`
	Rate        float64        `json:"rate"`
	DurationS   float64        `json:"duration_s"`
	Concurrency int            `json:"concurrency"`
	MaxTokens   int            `json:"max_tokens"`
	Requests    int            `json:"requests"`
	Completed   int            `json:"completed"`
	Errors      int            `json:"errors"`
	WallP50Ms   float64        `json:"wall_p50_ms"`
	WallP99Ms   float64        `json:"wall_p99_ms"`
	TTFTP50Ms   float64        `json:"ttft_p50_ms"`
	TTFTP99Ms   float64        `json:"ttft_p99_ms"`
	QueueP99Ms  float64        `json:"queue_p99_ms"`
	Preemptions int            `json:"preemptions"`
	Server      map[string]any `json:"server"`
	Rows        []record       `json:"rows"`
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "scheduler base URL")
	rate := flag.Float64("rate", 2.0, "offered requests per second")
	duration := flag.Duration("duration", 30*time.Second, "run duration")
	concurrency := flag.Int("concurrency", 32, "worker goroutines")
	maxTokens := flag.Int("max-tokens", 128, "tokens per request")
	prompt := flag.String("prompt", "Explain how a write-ahead log works and why it is written before the data pages.", "prompt")
	promptFile := flag.String("prompt-file", "", "read the prompt from this file")
	tenant := flag.String("tenant", "load", "tenant name")
	priority := flag.Int("priority", 0, "request priority (lower is more important)")
	weight := flag.Float64("weight", 1.0, "tenant weight for WFQ")
	out := flag.String("out", "", "output JSON")
	flag.Parse()
	if *promptFile != "" {
		data, err := os.ReadFile(*promptFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		*prompt = string(data)
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	// Buffer the offered load instead of blocking the arrival ticker on busy
	// workers. That is what makes the offered rate real: the proxy's queue is
	// allowed to grow, and 429s are counted rather than silently dropped.
	jobs := make(chan int, 65536)
	results := make(chan record, 4096)
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(*duration)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				results <- doOne(client, *url, id, *maxTokens, *prompt, *tenant, *priority, *weight)
			}
		}()
	}

	go func() {
		interval := time.Duration(float64(time.Second) / *rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		id := 0
		for now := range ticker.C {
			if now.After(deadline) {
				break
			}
			select {
			case jobs <- id:
				id++
			default:
				// The local buffer is full; the offered load is beyond what
				// this client can carry. Count it as an error, not a timeout.
				results <- record{ID: id, Status: 0, Error: "loadgen buffer full"}
				id++
			}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	rows := make([]record, 0, 1024)
	for row := range results {
		rows = append(rows, row)
	}
	elapsed := time.Since(start).Seconds()

	wall := make([]float64, 0, len(rows))
	ttft := make([]float64, 0, len(rows))
	queue := make([]float64, 0, len(rows))
	completed, errors, preemptions := 0, 0, 0
	for _, row := range rows {
		if row.Status == 200 {
			completed++
			wall = append(wall, row.WallMs)
			ttft = append(ttft, row.TTFTMs)
			queue = append(queue, row.QueueMs)
		} else {
			errors++
		}
		preemptions += row.Preemptions
	}

	art := artifact{
		Schema:      "kv-scheduler/loadgen/v1",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		URL:         *url,
		Rate:        *rate,
		DurationS:   elapsed,
		Concurrency: *concurrency,
		MaxTokens:   *maxTokens,
		Requests:    len(rows),
		Completed:   completed,
		Errors:      errors,
		WallP50Ms:   quantile(wall, 0.50),
		WallP99Ms:   quantile(wall, 0.99),
		TTFTP50Ms:   quantile(ttft, 0.50),
		TTFTP99Ms:   quantile(ttft, 0.99),
		QueueP99Ms:  quantile(queue, 0.99),
		Preemptions: preemptions,
		Rows:        rows,
	}
	if resp, err := client.Get(*url + "/stats"); err == nil {
		defer resp.Body.Close()
		var server map[string]any
		if json.NewDecoder(resp.Body).Decode(&server) == nil {
			art.Server = server
			if metrics, ok := server["metrics"].(map[string]any); ok {
				if value, ok := metrics["preempted"].(float64); ok {
					art.Preemptions = int(value)
				}
			}
		}
	}

	if *out == "" {
		*out = fmt.Sprintf("results/loadgen_%s.json", time.Now().UTC().Format("20060102T150405Z"))
	}
	data, _ := json.MarshalIndent(art, "", "  ")
	if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("rate=%.2f req/s  requests=%d completed=%d errors=%d  wall p50/p99=%.0f/%.0f ms  ttft p50/p99=%.0f/%.0f ms  preemptions=%d\n",
		*rate, len(rows), completed, errors, art.WallP50Ms, art.WallP99Ms, art.TTFTP50Ms, art.TTFTP99Ms, preemptions)
	fmt.Println("wrote", *out)
}

func doOne(client *http.Client, base string, id, maxTokens int, prompt, tenant string, priority int, weight float64) record {
	body, _ := json.Marshal(map[string]any{
		"prompt":       prompt,
		"n_predict":    maxTokens,
		"temperature":  0,
		"top_k":        1,
		"seed":         1000 + id,
		"tenant":       tenant,
		"priority":     priority,
		"weight":       weight,
		"ignore_eos":   true,
		"cache_prompt": false,
	})
	started := time.Now()
	resp, err := client.Post(base+"/completion", "application/json", bytes.NewReader(body))
	if err != nil {
		return record{ID: id, Status: 0, WallMs: float64(time.Since(started)) / float64(time.Millisecond), Error: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	row := record{
		ID:          id,
		Status:      resp.StatusCode,
		WallMs:      float64(time.Since(started)) / float64(time.Millisecond),
		QueueMs:     headerFloat(resp, "X-Scheduler-Queue-Ms"),
		TTFTMs:      headerFloat(resp, "X-Scheduler-TTFT-Ms"),
		Preemptions: int(headerFloat(resp, "X-Scheduler-Preemptions")),
	}
	var payload struct {
		Usage struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
		Timings struct {
			PredictedN int `json:"predicted_n"`
		} `json:"timings"`
	}
	if json.Unmarshal(raw, &payload) == nil {
		row.Tokens = payload.Usage.CompletionTokens
		if row.Tokens == 0 {
			row.Tokens = payload.Timings.PredictedN
		}
	}
	if resp.StatusCode != 200 {
		row.Error = string(raw)
	}
	return row
}

func headerFloat(resp *http.Response, key string) float64 {
	value, err := strconv.ParseFloat(resp.Header.Get(key), 64)
	if err != nil {
		return 0
	}
	return value
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
	return sorted[lo] + (pos-float64(lo))*(sorted[hi]-sorted[lo])
}
