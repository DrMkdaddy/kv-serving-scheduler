// Package sched contains the admission, batching, and preemption policies
// shared by the simulator and the real proxy.
package sched

import "time"

// Request is one unit of work. PromptTokens and MaxTokens are counts from the
// tokenizer, not bytes and not characters.
type Request struct {
	ID           string  `json:"id"`
	Tenant       string  `json:"tenant"`
	PromptTokens int     `json:"prompt_tokens"`
	MaxTokens    int     `json:"max_tokens"`
	ArrivalMs    float64 `json:"arrival_ms"`
	Priority     int     `json:"priority"`
	Weight       float64 `json:"weight"`
	SLO          SLO     `json:"slo"`
}

// SLO is the goodput contract for one request.
type SLO struct {
	TTFTMs float64 `json:"ttft_ms"`
	ITLMs  float64 `json:"itl_ms"`
}

// Result is the per-request record written to the artifact.
type Result struct {
	ID           string    `json:"id"`
	Tenant       string    `json:"tenant"`
	ArrivalMs    float64   `json:"arrival_ms"`
	AdmittedMs   float64   `json:"admitted_ms"`
	FirstTokenMs float64   `json:"first_token_ms"`
	CompletedMs  float64   `json:"completed_ms"`
	TTFTMs       float64   `json:"ttft_ms"`
	WaitMs       float64   `json:"wait_ms"`
	ITLMs        []float64 `json:"itl_ms"`
	Tokens       int       `json:"tokens"`
	Preemptions  int       `json:"preemptions"`
	Failed       bool      `json:"failed"`
	Goodput      bool      `json:"goodput"`
	PromptTokens int       `json:"prompt_tokens"`
}

// TenantStats is the per-tenant view used for the fairness claim.
type TenantStats struct {
	OfferedTokens   int     `json:"offered_tokens"`
	CompletedTokens int     `json:"completed_tokens"`
	CompletionRatio float64 `json:"completion_ratio"`
	OfferedRequests int     `json:"offered_requests"`
	Completed       int     `json:"completed"`
	Preemptions     int     `json:"preemptions"`
	TTFTP99Ms       float64 `json:"ttft_p99_ms"`
	WaitP99Ms       float64 `json:"wait_p99_ms"`
}

// Fairness is the Jain index over completed tokens and over completion ratios.
// Raw token Jain is not meaningful when tenants offer different demand, so the
// completion-ratio index is the one the writeup uses.
type Fairness struct {
	Jain           float64                `json:"jain_tokens"`
	JainCompletion float64                `json:"jain_completion_ratio"`
	Tokens         map[string]int         `json:"tokens"`
	Requests       map[string]int         `json:"requests"`
	Preemptions    map[string]int         `json:"preemptions"`
	Tenants        map[string]TenantStats `json:"tenants"`
}

// Sample is one point in the KV/queue time series.
type Sample struct {
	TimeMs       float64 `json:"time_ms"`
	Active       int     `json:"active"`
	Queued       int     `json:"queued"`
	UsedBlocks   int     `json:"used_blocks"`
	FreeBlocks   int     `json:"free_blocks"`
	WasteTokens  int     `json:"waste_tokens"`
	Preemptions  int     `json:"preemptions"`
	DecodeTokens int     `json:"decode_tokens"`
	QueueWaitP95 float64 `json:"queue_wait_p95_ms"`
	BatchSize    int     `json:"batch_size"`
	DecodeStepMs float64 `json:"decode_step_ms"`
}

// Summary is the aggregate result for one scenario.
type Summary struct {
	Name           string   `json:"name"`
	Policy         string   `json:"policy"`
	Preempt        string   `json:"preempt"`
	Slots          int      `json:"slots"`
	BlockSize      int      `json:"block_size"`
	TotalBlocks    int      `json:"total_blocks"`
	OfferedLoad    float64  `json:"offered_load"`
	Requests       int      `json:"requests"`
	Completed      int      `json:"completed"`
	Failed         int      `json:"failed"`
	Preemptions    int      `json:"preemptions"`
	ThroughputTPS  float64  `json:"throughput_tps"`
	RequestTPS     float64  `json:"request_tps"`
	Goodput        float64  `json:"goodput"`
	TTFTP50Ms      float64  `json:"ttft_p50_ms"`
	TTFTP99Ms      float64  `json:"ttft_p99_ms"`
	ITLP50Ms       float64  `json:"itl_p50_ms"`
	ITLP99Ms       float64  `json:"itl_p99_ms"`
	WaitP50Ms      float64  `json:"wait_p50_ms"`
	WaitP99Ms      float64  `json:"wait_p99_ms"`
	DurationMs     float64  `json:"duration_ms"`
	AvgActive      float64  `json:"avg_active"`
	AvgUsedBlocks  float64  `json:"avg_used_blocks"`
	PeakUsedBlocks int      `json:"peak_used_blocks"`
	AvgWasteTokens float64  `json:"avg_waste_tokens"`
	Fairness       Fairness `json:"fairness"`
	Samples        []Sample `json:"samples"`
	RequestResults []Result `json:"request_results"`
}

// Millis converts a duration to float milliseconds.
func Millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
