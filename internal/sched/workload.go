package sched

import (
	"fmt"
	"math"
	"math/rand"
)

// GenerateWorkload produces requests with exponential inter-arrival times.
// The same seed and config always produce the same stream, which is what makes
// a policy comparison reproducible.
func GenerateWorkload(cfg WorkloadConfig) ([]Request, error) {
	if cfg.Requests <= 0 {
		return nil, fmt.Errorf("requests must be positive")
	}
	if cfg.ArrivalRate <= 0 {
		return nil, fmt.Errorf("arrival_rate must be positive")
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("at least one tenant is required")
	}
	if cfg.PromptMax < cfg.PromptMin || cfg.OutputMax < cfg.OutputMin {
		return nil, fmt.Errorf("invalid token ranges")
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	requests := make([]Request, 0, cfg.Requests)
	now := 0.0
	for i := 0; i < cfg.Requests; i++ {
		// Exponential inter-arrival: -ln(U)/lambda seconds, converted to ms.
		u := rng.Float64()
		if u == 0 {
			u = 1e-12
		}
		now += -1000.0 * math.Log(u) / cfg.ArrivalRate
		tenantIndex := i % len(cfg.Tenants)
		if len(cfg.TenantMix) == len(cfg.Tenants) {
			r := rng.Float64()
			acc := 0.0
			for j, share := range cfg.TenantMix {
				acc += share
				if r <= acc {
					tenantIndex = j
					break
				}
			}
		}
		tenant := cfg.Tenants[tenantIndex]
		weight := 1.0
		if len(cfg.Weights) > 0 {
			weight = cfg.Weights[tenantIndex%len(cfg.Weights)]
		}
		priority := 0
		if len(cfg.TenantPriority) == len(cfg.Tenants) {
			priority = cfg.TenantPriority[tenantIndex]
		} else if len(cfg.PriorityMix) > 0 {
			priority = cfg.PriorityMix[i%len(cfg.PriorityMix)]
		}
		requests = append(requests, Request{
			ID:           fmt.Sprintf("r%05d", i),
			Tenant:       tenant,
			PromptTokens: cfg.PromptMin + rng.Intn(cfg.PromptMax-cfg.PromptMin+1),
			MaxTokens:    cfg.OutputMin + rng.Intn(cfg.OutputMax-cfg.OutputMin+1),
			ArrivalMs:    now,
			Priority:     priority,
			Weight:       weight,
			SLO: SLO{
				TTFTMs: cfg.SLOTTFTPMs,
				ITLMs:  cfg.SLOITLMs,
			},
		})
	}
	return requests, nil
}
