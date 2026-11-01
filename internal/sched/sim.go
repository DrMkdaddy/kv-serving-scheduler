package sched

import (
	"fmt"
	"math"
	"sort"

	"github.com/DrMkdaddy/kv-serving-scheduler/internal/kv"
)

type queueItem struct {
	req             *Request
	generated       int
	preemptions     int
	firstTokenMs    float64
	lastTokenMs     float64
	hasFirst        bool
	firstAdmittedMs float64
	wasAdmitted     bool
	enqueuedAt      float64
	virtualEnd      float64
	restoreMs       float64
}

type activeSeq struct {
	item         *queueItem
	admittedAt   float64
	prefillUntil float64
	lastTokenAt  float64
	itls         []float64
}

type simulator struct {
	cfg       Config
	sc        Scenario
	alloc     *kv.Allocator
	queue     []*queueItem
	active    map[string]*activeSeq
	now       float64
	arrivals  []Request
	next      int
	results   []Result
	preempted int
	failed    int
	slots     int

	tenantVT            map[string]float64
	tenantTokens        map[string]int
	tenantReqs          map[string]int
	tenantPreempt       map[string]int
	tenantOfferedTokens map[string]int
	tenantOfferedReqs   map[string]int
	tenantTTFT          map[string][]float64
	tenantWait          map[string][]float64

	decodeTokens int
	samples      []Sample
	lastSample   float64
	peakBlocks   int
	sumActive    float64
	sumBlocks    float64
	sumWaste     float64
	nSamples     int
}

// RunScenario executes one open-loop scenario and returns its summary.
func RunScenario(cfg Config, sc Scenario) (Summary, error) {
	reqs, err := GenerateWorkload(sc.Workload)
	if err != nil {
		return Summary{}, err
	}
	alloc, err := kv.New(sc.Scheduler.BlockSize, sc.Scheduler.TotalBlocks)
	if err != nil {
		return Summary{}, err
	}
	sim := &simulator{
		cfg:                 cfg,
		sc:                  sc,
		alloc:               alloc,
		active:              make(map[string]*activeSeq),
		arrivals:            reqs,
		slots:               sc.Scheduler.Slots,
		tenantVT:            make(map[string]float64),
		tenantTokens:        make(map[string]int),
		tenantReqs:          make(map[string]int),
		tenantPreempt:       make(map[string]int),
		tenantOfferedTokens: make(map[string]int),
		tenantOfferedReqs:   make(map[string]int),
		tenantTTFT:          make(map[string][]float64),
		tenantWait:          make(map[string][]float64),
		lastSample:          -1,
	}
	if err := sim.run(); err != nil {
		return Summary{}, err
	}
	return sim.summary(), nil
}

func (s *simulator) run() error {
	maxDuration := s.sc.Workload.MaxDurationMs
	if maxDuration <= 0 {
		maxDuration = 1e12
	}
	// Cap the loop so a policy bug cannot hang the process.
	for step := 0; step < 2_000_000; step++ {
		s.processArrivals()
		s.admit()
		if len(s.active) == 0 && s.next >= len(s.arrivals) && len(s.queue) == 0 {
			break
		}
		if s.now > maxDuration {
			break
		}
		if len(s.active) == 0 {
			// Nothing resident: advance to the next arrival rather than
			// spinning on zero-cost steps.
			if s.next < len(s.arrivals) {
				s.now = math.Max(s.now, s.arrivals[s.next].ArrivalMs)
				continue
			}
			break
		}
		s.step()
	}
	return nil
}

func (s *simulator) processArrivals() {
	for s.next < len(s.arrivals) && s.arrivals[s.next].ArrivalMs <= s.now {
		req := s.arrivals[s.next]
		s.next++
		s.tenantOfferedTokens[req.Tenant] += req.PromptTokens + req.MaxTokens
		s.tenantOfferedReqs[req.Tenant]++
		if len(s.queue) >= s.sc.Scheduler.MaxQueue {
			s.failed++
			s.tenantReqs[req.Tenant]++
			s.results = append(s.results, Result{
				ID: req.ID, Tenant: req.Tenant, ArrivalMs: req.ArrivalMs,
				Failed: true, PromptTokens: req.PromptTokens,
			})
			continue
		}
		item := &queueItem{req: &req, enqueuedAt: s.now}
		s.setVirtualEnd(item)
		s.queue = append(s.queue, item)
	}
}

func (s *simulator) setVirtualEnd(item *queueItem) {
	if s.sc.Scheduler.Policy != "wfq" {
		item.virtualEnd = item.req.ArrivalMs
		return
	}
	cost := float64(item.req.PromptTokens + item.req.MaxTokens)
	weight := item.req.Weight
	if weight <= 0 {
		weight = 1
	}
	start := math.Max(item.req.ArrivalMs, s.tenantVT[item.req.Tenant])
	item.virtualEnd = start + cost/weight
	s.tenantVT[item.req.Tenant] = item.virtualEnd
}

func (s *simulator) less(a, b *queueItem) bool {
	switch s.sc.Scheduler.Policy {
	case "priority":
		if a.req.Priority != b.req.Priority {
			return a.req.Priority < b.req.Priority
		}
		return a.req.ArrivalMs < b.req.ArrivalMs
	case "wfq":
		if a.virtualEnd != b.virtualEnd {
			return a.virtualEnd < b.virtualEnd
		}
		return a.req.ArrivalMs < b.req.ArrivalMs
	default:
		return a.enqueuedAt < b.enqueuedAt
	}
}

func (s *simulator) admit() {
	for len(s.active) < s.slots && len(s.queue) > 0 {
		idx := -1
		var chosen *queueItem
		for i, item := range s.queue {
			need := item.req.PromptTokens + item.generated
			if need > s.sc.Scheduler.TotalBlocks*s.sc.Scheduler.BlockSize {
				// This request can never fit; fail it now.
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				s.fail(item)
				idx = -2
				break
			}
			if !s.alloc.CanFit(need) {
				continue
			}
			if chosen == nil || s.less(item, chosen) {
				chosen = item
				idx = i
			}
		}
		if idx == -2 {
			continue
		}
		if chosen == nil {
			return
		}
		s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
		if !s.alloc.Alloc(chosen.req.ID, chosen.req.PromptTokens+chosen.generated) {
			// Raced with another admission; put it back and stop.
			s.queue = append(s.queue, chosen)
			return
		}
		prefillTokens := chosen.req.PromptTokens + chosen.generated
		prefillMs := 0.0
		if s.sc.Scheduler.Preempt == "swap" && chosen.restoreMs > 0 {
			prefillMs = chosen.restoreMs
		} else {
			prefillMs = 1000 * float64(prefillTokens) / s.cfg.Calibration.PrefillTPS
		}
		if !chosen.wasAdmitted {
			chosen.firstAdmittedMs = s.now
			chosen.wasAdmitted = true
		}
		seq := &activeSeq{
			item:         chosen,
			admittedAt:   s.now,
			prefillUntil: s.now + prefillMs,
			lastTokenAt:  s.now,
		}
		if chosen.hasFirst {
			seq.lastTokenAt = chosen.lastTokenMs
		}
		s.active[chosen.req.ID] = seq
	}
}

func (s *simulator) fail(item *queueItem) {
	s.failed++
	s.tenantReqs[item.req.Tenant]++
	s.results = append(s.results, Result{
		ID: item.req.ID, Tenant: item.req.Tenant, ArrivalMs: item.req.ArrivalMs,
		Failed: true, PromptTokens: item.req.PromptTokens,
		Preemptions: item.preemptions,
	})
}

func (s *simulator) step() {
	decoding := make([]*activeSeq, 0, len(s.active))
	readyAt := math.Inf(1)
	for _, seq := range s.active {
		if seq.prefillUntil <= s.now {
			decoding = append(decoding, seq)
		} else if seq.prefillUntil < readyAt {
			readyAt = seq.prefillUntil
		}
	}
	if len(decoding) == 0 {
		// Everyone is prefilling. Advance to the earliest completion, but do
		// not run past the next arrival.
		next := readyAt
		if s.next < len(s.arrivals) && s.arrivals[s.next].ArrivalMs < next {
			next = s.arrivals[s.next].ArrivalMs
		}
		if next > s.now {
			s.now = next
		}
		return
	}
	stepMs := s.cfg.Calibration.StepMs(len(decoding))
	s.sample(stepMs, len(decoding))

	for _, seq := range decoding {
		if _, ok := s.active[seq.item.req.ID]; !ok {
			// An earlier grow in this step preempted this sequence.
			continue
		}
		newTokens := seq.item.req.PromptTokens + seq.item.generated + 1
		if !s.alloc.Grow(seq.item.req.ID, newTokens) {
			if !s.preemptAndRetry(seq, newTokens) {
				continue
			}
		}
		finishAt := s.now + stepMs
		seq.item.generated++
		s.decodeTokens++
		if !seq.item.hasFirst {
			seq.item.hasFirst = true
			seq.item.firstTokenMs = finishAt
		} else {
			seq.itls = append(seq.itls, finishAt-seq.lastTokenAt)
		}
		seq.item.lastTokenMs = finishAt
		seq.lastTokenAt = finishAt
		if seq.item.generated >= seq.item.req.MaxTokens {
			s.complete(seq, finishAt)
		}
	}
	s.now += stepMs
}

func (s *simulator) preemptAndRetry(seq *activeSeq, wantTokens int) bool {
	if s.sc.Scheduler.Preempt == "none" {
		s.freeActive(seq)
		s.fail(seq.item)
		return false
	}
	for attempt := 0; attempt < 4; attempt++ {
		victim := s.chooseVictim(seq.item.req.ID)
		if victim == nil {
			return false
		}
		s.preempt(victim)
		if s.alloc.Grow(seq.item.req.ID, wantTokens) {
			return true
		}
		if _, stillActive := s.active[seq.item.req.ID]; !stillActive {
			return false
		}
	}
	return false
}

func (s *simulator) chooseVictim(exclude string) *activeSeq {
	var best *activeSeq
	for id, seq := range s.active {
		if id == exclude && len(s.active) == 1 {
			// A single request can still be preempted when its own grow
			// fails; that is the "recompute from a shorter prefix" case.
		}
		if best == nil {
			best = seq
			continue
		}
		better := false
		switch s.sc.Scheduler.Victim {
		case "largest":
			better = s.alloc.Blocks(seq.item.req.ID) > s.alloc.Blocks(best.item.req.ID)
		case "newest":
			better = seq.admittedAt > best.admittedAt
		case "oldest":
			better = seq.admittedAt < best.admittedAt
		case "tenant_share":
			better = s.activeBlocks(seq.item.req.Tenant) > s.activeBlocks(best.item.req.Tenant)
		default: // lowest_priority
			if seq.item.req.Priority != best.item.req.Priority {
				better = seq.item.req.Priority > best.item.req.Priority
			} else {
				better = seq.admittedAt > best.admittedAt
			}
		}
		if better {
			best = seq
		}
	}
	return best
}

func (s *simulator) preempt(victim *activeSeq) {
	blocks := s.alloc.Blocks(victim.item.req.ID)
	s.freeActive(victim)
	s.preempted++
	victim.item.preemptions++
	s.tenantPreempt[victim.item.req.Tenant]++
	restore := 0.0
	if s.sc.Scheduler.Preempt == "swap" {
		restore = s.cfg.Calibration.SwapMsPerBlock * float64(blocks)
	}
	item := &queueItem{
		req:             victim.item.req,
		generated:       victim.item.generated,
		preemptions:     victim.item.preemptions,
		firstTokenMs:    victim.item.firstTokenMs,
		lastTokenMs:     victim.item.lastTokenMs,
		hasFirst:        victim.item.hasFirst,
		firstAdmittedMs: victim.item.firstAdmittedMs,
		wasAdmitted:     victim.item.wasAdmitted,
		enqueuedAt:      s.now,
		restoreMs:       restore,
	}
	s.setVirtualEnd(item)
	if len(s.queue) >= s.sc.Scheduler.MaxQueue {
		s.fail(item)
		return
	}
	s.queue = append(s.queue, item)
}

func (s *simulator) freeActive(seq *activeSeq) {
	s.alloc.Free(seq.item.req.ID)
	delete(s.active, seq.item.req.ID)
}

func (s *simulator) complete(seq *activeSeq, now float64) {
	s.alloc.Free(seq.item.req.ID)
	delete(s.active, seq.item.req.ID)
	itls := append([]float64(nil), seq.itls...)
	ttft := seq.item.firstTokenMs - seq.item.req.ArrivalMs
	if ttft < 0 {
		ttft = 0
	}
	good := ttft <= seq.item.req.SLO.TTFTMs && percentile(itls, 0.95) <= seq.item.req.SLO.ITLMs
	s.results = append(s.results, Result{
		ID:           seq.item.req.ID,
		Tenant:       seq.item.req.Tenant,
		ArrivalMs:    seq.item.req.ArrivalMs,
		AdmittedMs:   seq.item.firstAdmittedMs,
		FirstTokenMs: seq.item.firstTokenMs,
		CompletedMs:  now,
		TTFTMs:       ttft,
		WaitMs:       seq.item.firstAdmittedMs - seq.item.req.ArrivalMs,
		ITLMs:        itls,
		Tokens:       seq.item.generated,
		Preemptions:  seq.item.preemptions,
		Goodput:      good,
		PromptTokens: seq.item.req.PromptTokens,
	})
	s.tenantTokens[seq.item.req.Tenant] += seq.item.generated
	s.tenantReqs[seq.item.req.Tenant]++
	s.tenantTTFT[seq.item.req.Tenant] = append(s.tenantTTFT[seq.item.req.Tenant], ttft)
	s.tenantWait[seq.item.req.Tenant] = append(s.tenantWait[seq.item.req.Tenant], seq.item.firstAdmittedMs-seq.item.req.ArrivalMs)
}

func (s *simulator) activeBlocks(tenant string) int {
	blocks := 0
	for _, seq := range s.active {
		if seq.item.req.Tenant == tenant {
			blocks += s.alloc.Blocks(seq.item.req.ID)
		}
	}
	return blocks
}

func (s *simulator) sample(stepMs float64, batch int) {
	if s.lastSample >= 0 && s.now-s.lastSample < 100 {
		return
	}
	st := s.alloc.Stats()
	waits := make([]float64, 0, len(s.queue))
	for _, item := range s.queue {
		waits = append(waits, s.now-item.enqueuedAt)
	}
	s.samples = append(s.samples, Sample{
		TimeMs:       s.now,
		Active:       len(s.active),
		Queued:       len(s.queue),
		UsedBlocks:   st.UsedBlocks,
		FreeBlocks:   st.FreeBlocks,
		WasteTokens:  st.InternalWasteTokens,
		Preemptions:  s.preempted,
		DecodeTokens: s.decodeTokens,
		QueueWaitP95: percentile(waits, 0.95),
		BatchSize:    batch,
		DecodeStepMs: stepMs,
	})
	s.lastSample = s.now
	s.peakBlocks = maxInt(s.peakBlocks, st.UsedBlocks)
	s.sumActive += float64(len(s.active))
	s.sumBlocks += float64(st.UsedBlocks)
	s.sumWaste += float64(st.InternalWasteTokens)
	s.nSamples++
}

func (s *simulator) summary() Summary {
	completed := make([]Result, 0, len(s.results))
	ttfts := make([]float64, 0, len(s.results))
	waits := make([]float64, 0)
	var itls []float64
	tokens := 0
	good := 0
	for _, r := range s.results {
		if r.Failed {
			continue
		}
		completed = append(completed, r)
		ttfts = append(ttfts, r.TTFTMs)
		waits = append(waits, r.WaitMs)
		itls = append(itls, r.ITLMs...)
		tokens += r.Tokens
		if r.Goodput {
			good++
		}
	}
	duration := s.now
	if duration <= 0 {
		duration = 1
	}
	summary := Summary{
		Name:           s.sc.Name,
		Policy:         s.sc.Scheduler.Policy,
		Preempt:        s.sc.Scheduler.Preempt,
		Slots:          s.sc.Scheduler.Slots,
		BlockSize:      s.sc.Scheduler.BlockSize,
		TotalBlocks:    s.sc.Scheduler.TotalBlocks,
		OfferedLoad:    s.sc.OfferedLoad,
		Requests:       len(s.arrivals),
		Completed:      len(completed),
		Failed:         s.failed,
		Preemptions:    s.preempted,
		ThroughputTPS:  float64(tokens) / (duration / 1000),
		RequestTPS:     float64(len(completed)) / (duration / 1000),
		TTFTP50Ms:      percentile(ttfts, 0.50),
		TTFTP99Ms:      percentile(ttfts, 0.99),
		ITLP50Ms:       percentile(itls, 0.50),
		ITLP99Ms:       percentile(itls, 0.99),
		WaitP50Ms:      percentile(waits, 0.50),
		WaitP99Ms:      percentile(waits, 0.99),
		DurationMs:     duration,
		PeakUsedBlocks: s.peakBlocks,
		Samples:        s.samples,
		RequestResults: completed,
	}
	if len(s.results) > 0 {
		// Failed requests are not goodput. Counting only completed requests
		// would make a shedding policy look better than it is.
		summary.Goodput = float64(good) / float64(len(s.results))
	}
	if s.nSamples > 0 {
		summary.AvgActive = s.sumActive / float64(s.nSamples)
		summary.AvgUsedBlocks = s.sumBlocks / float64(s.nSamples)
		summary.AvgWasteTokens = s.sumWaste / float64(s.nSamples)
	}
	tenants := make(map[string]TenantStats)
	ratios := make([]float64, 0, len(s.sc.Workload.Tenants))
	for _, tenant := range s.sc.Workload.Tenants {
		offered := s.tenantOfferedTokens[tenant]
		completed := s.tenantTokens[tenant]
		ratio := 0.0
		if offered > 0 {
			ratio = float64(completed) / float64(offered)
		}
		ratios = append(ratios, ratio)
		tenants[tenant] = TenantStats{
			OfferedTokens:   offered,
			CompletedTokens: completed,
			CompletionRatio: ratio,
			OfferedRequests: s.tenantOfferedReqs[tenant],
			Completed:       s.tenantReqs[tenant],
			Preemptions:     s.tenantPreempt[tenant],
			TTFTP99Ms:       percentile(s.tenantTTFT[tenant], 0.99),
			WaitP99Ms:       percentile(s.tenantWait[tenant], 0.99),
		}
	}
	summary.Fairness = Fairness{
		Jain:           jain(s.tenantTokens),
		JainCompletion: jainFloat(ratios),
		Tokens:         copyIntMap(s.tenantTokens),
		Requests:       copyIntMap(s.tenantReqs),
		Preemptions:    copyIntMap(s.tenantPreempt),
		Tenants:        tenants,
	}
	return summary
}

func percentile(values []float64, p float64) float64 {
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
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func jain(values map[string]int) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum, sq float64
	for _, v := range values {
		sum += float64(v)
		sq += float64(v * v)
	}
	if sq == 0 {
		return 0
	}
	return sum * sum / (float64(len(values)) * sq)
}

func jainFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum, sq float64
	for _, v := range values {
		sum += v
		sq += v * v
	}
	if sq == 0 {
		return 0
	}
	return sum * sum / (float64(len(values)) * sq)
}

func copyIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Sanity check that a scenario with one slot and enough blocks completes.
func ValidateScenario(cfg Config, sc Scenario) error {
	if sc.Scheduler.Slots < 1 {
		return fmt.Errorf("slots must be at least 1")
	}
	return nil
}
