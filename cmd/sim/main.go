package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/DrMkdaddy/kv-serving-scheduler/internal/sched"
)

type artifact struct {
	Schema      string            `json:"schema"`
	GeneratedAt string            `json:"generated_at"`
	Config      string            `json:"config"`
	Description string            `json:"description"`
	GitCommit   string            `json:"git_commit"`
	Calibration sched.Calibration `json:"calibration"`
	Scenarios   []sched.Summary   `json:"scenarios"`
}

func main() {
	configPath := flag.String("config", "configs/qwen3-4b-calibrated.json", "simulator config")
	outPath := flag.String("out", "", "output JSON (default: results/sim_<timestamp>.json)")
	only := flag.String("scenario", "", "run only this scenario name")
	flag.Parse()

	cfg, err := sched.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	if *outPath == "" {
		*outPath = filepath.Join("results", "sim_"+time.Now().UTC().Format("20060102T150405Z")+".json")
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	scenarios := make([]sched.Summary, 0, len(cfg.Scenarios))
	for _, sc := range cfg.Scenarios {
		if *only != "" && sc.Name != *only {
			continue
		}
		summary, err := sched.RunScenario(cfg, sc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scenario %s: %v\n", sc.Name, err)
			os.Exit(1)
		}
		scenarios = append(scenarios, summary)
		fmt.Printf("%-18s policy=%-8s preempt=%-9s load=%.2f  req/s=%6.2f tok/s=%7.1f  TTFT p50/p99=%6.0f/%7.0f ms  ITL p99=%5.1f ms  preempt=%d  goodput=%5.1f%%\n",
			summary.Name, summary.Policy, summary.Preempt, summary.OfferedLoad,
			summary.RequestTPS, summary.ThroughputTPS,
			summary.TTFTP50Ms, summary.TTFTP99Ms, summary.ITLP99Ms,
			summary.Preemptions, summary.Goodput*100)
	}
	if len(scenarios) == 0 {
		fmt.Fprintln(os.Stderr, "no scenarios matched")
		os.Exit(2)
	}

	art := artifact{
		Schema:      "kv-scheduler/sim/v1",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Config:      *configPath,
		Description: cfg.Description,
		GitCommit:   gitCommit(),
		Calibration: cfg.Calibration,
		Scenarios:   scenarios,
	}
	data, err := json.MarshalIndent(art, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, append(data, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote", *outPath)
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return string(out[:len(out)-1])
}
