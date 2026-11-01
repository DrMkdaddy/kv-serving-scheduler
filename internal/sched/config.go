package sched

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// CalibrationPoint is one measured (batch size, decode step time) pair.
type CalibrationPoint struct {
	N    int     `json:"n"`
	Step float64 `json:"step_ms"`
}

// Calibration is the measured service model used by the simulator.
type Calibration struct {
	PrefillTPS     float64            `json:"prefill_tps"`
	DecodeStepMs   []CalibrationPoint `json:"decode_step_ms"`
	SwapMsPerBlock float64            `json:"swap_ms_per_block"`
	Source         string             `json:"source"`
}

func (c Calibration) StepMs(batch int) float64 {
	if batch <= 0 {
		return 0
	}
	if len(c.DecodeStepMs) == 0 {
		return 10
	}
	points := append([]CalibrationPoint(nil), c.DecodeStepMs...)
	sort.Slice(points, func(i, j int) bool { return points[i].N < points[j].N })
	if batch <= points[0].N {
		return points[0].Step
	}
	for i := 1; i < len(points); i++ {
		if batch <= points[i].N {
			// Linear interpolation between measured points.
			a, b := points[i-1], points[i]
			frac := float64(batch-a.N) / float64(b.N-a.N)
			return a.Step + frac*(b.Step-a.Step)
		}
	}
	// Extrapolate with the slope of the last segment.
	last := points[len(points)-1]
	prev := points[len(points)-2]
	slope := (last.Step - prev.Step) / float64(last.N-prev.N)
	return last.Step + slope*float64(batch-last.N)
}

// SchedulerConfig controls admission and preemption.
type SchedulerConfig struct {
	Slots       int    `json:"slots"`
	BlockSize   int    `json:"block_size"`
	TotalBlocks int    `json:"total_blocks"`
	MaxQueue    int    `json:"max_queue"`
	Policy      string `json:"policy"`  // fcfs, priority, wfq
	Preempt     string `json:"preempt"` // recompute, swap, none
	Victim      string `json:"victim"`  // lowest_priority, largest, newest, oldest, tenant_share
}

// WorkloadConfig describes the open-loop arrival process.
type WorkloadConfig struct {
	Requests       int       `json:"requests"`
	ArrivalRate    float64   `json:"arrival_rate"`
	PromptMin      int       `json:"prompt_min"`
	PromptMax      int       `json:"prompt_max"`
	OutputMin      int       `json:"output_min"`
	OutputMax      int       `json:"output_max"`
	Tenants        []string  `json:"tenants"`
	TenantMix      []float64 `json:"tenant_mix"`
	Seed           int64     `json:"seed"`
	PriorityMix    []int     `json:"priority_mix"`
	TenantPriority []int     `json:"tenant_priority"`
	Weights        []float64 `json:"weights"`
	SLOTTFTPMs     float64   `json:"slo_ttft_ms"`
	SLOITLMs       float64   `json:"slo_itl_ms"`
	MaxDurationMs  float64   `json:"max_duration_ms"`
}

// Scenario is one simulator run.
type Scenario struct {
	Name        string          `json:"name"`
	OfferedLoad float64         `json:"offered_load"`
	Scheduler   SchedulerConfig `json:"scheduler"`
	Workload    WorkloadConfig  `json:"workload"`
}

// Config is a complete simulator manifest.
type Config struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Calibration Calibration `json:"calibration"`
	Scenarios   []Scenario  `json:"scenarios"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Name == "" {
		return cfg, fmt.Errorf("config name is required")
	}
	if len(cfg.Scenarios) == 0 {
		return cfg, fmt.Errorf("at least one scenario is required")
	}
	if err := cfg.Calibration.Validate(); err != nil {
		return cfg, err
	}
	for i := range cfg.Scenarios {
		if err := cfg.Scenarios[i].Validate(); err != nil {
			return cfg, fmt.Errorf("scenario %d: %w", i, err)
		}
	}
	return cfg, nil
}

func (c Calibration) Validate() error {
	if c.PrefillTPS <= 0 {
		return fmt.Errorf("prefill_tps must be positive")
	}
	if len(c.DecodeStepMs) < 2 {
		return fmt.Errorf("need at least two decode_step_ms points")
	}
	for _, p := range c.DecodeStepMs {
		if p.N <= 0 || p.Step <= 0 {
			return fmt.Errorf("invalid calibration point %+v", p)
		}
	}
	return nil
}

func (s Scenario) Validate() error {
	if s.Scheduler.Slots <= 0 {
		return fmt.Errorf("slots must be positive")
	}
	if s.Scheduler.BlockSize <= 0 || s.Scheduler.TotalBlocks <= 0 {
		return fmt.Errorf("block_size and total_blocks must be positive")
	}
	if s.Scheduler.MaxQueue <= 0 {
		return fmt.Errorf("max_queue must be positive")
	}
	switch s.Scheduler.Policy {
	case "fcfs", "priority", "wfq":
	default:
		return fmt.Errorf("unknown policy %q", s.Scheduler.Policy)
	}
	switch s.Scheduler.Preempt {
	case "recompute", "swap", "none":
	default:
		return fmt.Errorf("unknown preempt %q", s.Scheduler.Preempt)
	}
	if s.Workload.Requests <= 0 || s.Workload.ArrivalRate <= 0 {
		return fmt.Errorf("workload requests and arrival_rate must be positive")
	}
	return nil
}
