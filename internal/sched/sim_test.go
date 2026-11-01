package sched

import "testing"

func testConfig() Config {
	return Config{
		Name: "test",
		Calibration: Calibration{
			PrefillTPS: 3000,
			DecodeStepMs: []CalibrationPoint{
				{N: 1, Step: 6},
				{N: 2, Step: 8},
				{N: 4, Step: 10},
				{N: 8, Step: 16},
			},
			SwapMsPerBlock: 0.5,
		},
	}
}

func testScenario(blocks int) Scenario {
	return Scenario{
		Name: "test",
		Scheduler: SchedulerConfig{
			Slots: 4, BlockSize: 256, TotalBlocks: blocks,
			MaxQueue: 64, Policy: "fcfs", Preempt: "recompute", Victim: "largest",
		},
		Workload: WorkloadConfig{
			Requests: 40, ArrivalRate: 4,
			PromptMin: 128, PromptMax: 512,
			OutputMin: 128, OutputMax: 256,
			Tenants: []string{"a", "b"}, Seed: 3,
			SLOTTFTPMs: 2000, SLOITLMs: 100,
			MaxDurationMs: 600000,
		},
	}
}

func TestGenerateDeterministic(t *testing.T) {
	cfg := testScenario(8).Workload
	a, err := GenerateWorkload(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateWorkload(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatal("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("request %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestStepInterpolation(t *testing.T) {
	cfg := testConfig()
	if got := cfg.Calibration.StepMs(3); got != 9 {
		t.Fatalf("step(3) = %v, want 9", got)
	}
	if got := cfg.Calibration.StepMs(16); got <= cfg.Calibration.StepMs(8) {
		t.Fatalf("extrapolation did not grow: %v", got)
	}
}

func TestLowLoadCompletes(t *testing.T) {
	cfg := testConfig()
	sc := testScenario(64)
	sc.Workload.Requests = 10
	sc.Workload.ArrivalRate = 0.5
	summary, err := RunScenario(cfg, sc)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Completed != 10 || summary.Failed != 0 {
		t.Fatalf("completed=%d failed=%d", summary.Completed, summary.Failed)
	}
	if summary.Preemptions != 0 {
		t.Fatalf("preemptions=%d, want 0 with a large pool", summary.Preemptions)
	}
}

func TestTightPoolPreemptsAndStillCompletes(t *testing.T) {
	cfg := testConfig()
	sc := testScenario(3)
	sc.Workload.Requests = 40
	sc.Workload.ArrivalRate = 8
	summary, err := RunScenario(cfg, sc)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Preemptions == 0 {
		t.Fatal("expected preemptions under a 3-block pool")
	}
	if summary.Completed != 40 {
		t.Fatalf("completed=%d, want 40 (recompute should finish them)", summary.Completed)
	}
	if summary.Fairness.JainCompletion == 0 {
		t.Fatal("missing fairness")
	}
}

func TestSheddingCountsFailures(t *testing.T) {
	cfg := testConfig()
	sc := testScenario(3)
	sc.Scheduler.Preempt = "none"
	sc.Workload.Requests = 40
	sc.Workload.ArrivalRate = 8
	summary, err := RunScenario(cfg, sc)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Failed == 0 {
		t.Fatal("expected failures with preempt=none and a tight pool")
	}
	if summary.Goodput > 1 {
		t.Fatalf("goodput out of range: %f", summary.Goodput)
	}
}

func TestPriorityProtectsTenant(t *testing.T) {
	cfg := testConfig()
	sc := testScenario(3)
	sc.Scheduler.Policy = "priority"
	sc.Workload.Requests = 80
	sc.Workload.ArrivalRate = 10
	sc.Workload.TenantMix = []float64{0.9, 0.1}
	sc.Workload.TenantPriority = []int{1, 0}
	summary, err := RunScenario(cfg, sc)
	if err != nil {
		t.Fatal(err)
	}
	a := summary.Fairness.Tenants["a"]
	b := summary.Fairness.Tenants["b"]
	if b.TTFTP99Ms == 0 {
		t.Fatal("tenant b has no completions")
	}
	if b.TTFTP99Ms > a.TTFTP99Ms {
		t.Fatalf("priority tenant slower: a=%f b=%f", a.TTFTP99Ms, b.TTFTP99Ms)
	}
}
