# kv-serving-scheduler

[![test](https://github.com/DrMkdaddy/kv-serving-scheduler/actions/workflows/test.yml/badge.svg)](https://github.com/DrMkdaddy/kv-serving-scheduler/actions/workflows/test.yml)

A serving scheduler for llama.cpp with a hard KV-cache budget: continuous
batching, block allocation, admission control, preemption, and per-tenant
fairness. Measurements run to 120% of capacity on one 16 GB GPU.

The repo has two paths that share the same allocator and policy code:

- `cmd/serve`: a Go proxy in front of `llama-server`. It queues requests,
  admits them against a block budget, pins them to a slot, streams the upstream
  response, and preempts (cancel + recompute) when the budget runs out.
- `cmd/sim`: a discrete-event simulator calibrated from measured llama.cpp
  service times. It runs policy sweeps the real server cannot expose.

## Quick start

```bash
go test ./...

# Policy sweep: FCFS, priority, WFQ at 80/100/120/150% of capacity
go run ./cmd/sim --config configs/qwen3-4b-calibrated.json --out results/sim.json

# Real proxy in front of llama-server
llama-server -m ... -c 8192 -np 4 -ngl 99 --metrics &
go run ./cmd/serve --config configs/qwen3-4b-real.json

# Load generator
go run ./cmd/loadgen --url http://127.0.0.1:8080 --rate 2 --duration 60s
```

Or run everything with `./scripts/reproduce.sh`.

## Results

### Simulator

4 slots, 8 blocks (2048 tokens), 200 requests. Source:
`results/sim_20261101.json`.

| Scenario | Req/s | Tok/s | TTFT p99 | Goodput |
|---|---:|---:|---:|---:|
| FCFS 80% | 1.57 | 303 | 2604 ms | 91.0% |
| FCFS 100% | 1.88 | 362 | 5463 ms | 66.5% |
| FCFS 120% | 1.89 | 364 | 14410 ms | 24.0% |
| FCFS 150% | 1.88 | 360 | 20045 ms | 11.0% |
| WFQ 120% | 1.85 | 357 | 13202 ms | 25.5% |
| Swap 120% | 2.04 | 392 | 8191 ms | 29.5% |
| No preemption 120% | 1.48 | 281 | 2440 ms | 59.5% |

At 120% load the system still completes requests, but p99 TTFT is 14.4 s and
24% meet both SLOs. Swapping instead of recomputing buys 8% throughput and 5.5
points of goodput. Shedding keeps successful requests fast (p99 2.4 s) but
fails 74 of 200.

### Real proxy

Four upstream slots, three logical KV blocks, 128 output tokens per request,
123-token prompt. Source: `results/real_summary.json`.

| Offered req/s | Completed | Tok/s | TTFT p99 | Wall p99 |
|---:|---:|---:|---:|---:|
| 1 | 19/19 | 122 | 55 ms | 949 ms |
| 2 | 39/39 | 210 | 3115 ms | 4562 ms |
| 3 | 59/59 | 219 | 8010 ms | 9602 ms |
| 4 | 79/79 | 218 | 8866 ms | 10318 ms |

Mixed priority at 3 req/s low plus 0.5 req/s high: the high tenant holds a
74 ms TTFT p99 while the low tenant sees 12.2 s and 14 preemptions.

The real proxy tops out near 219 tok/s: it reserves 3 blocks and uses the
per-token SSE path to measure TTFT/ITL. The direct llama-server calibration at
3 resident sequences is 359 tok/s. The simulator uses the direct curve, so its
absolute throughput is an upper bound.

## Design

1. **Block allocator.** KV memory is a fixed pool of `block_size`-token blocks.
   A sequence owns `ceil(tokens / block_size)` blocks. A failed grow keeps the
   old blocks and lets the caller decide whether to preempt.
2. **Admission.** A request carries a prompt-token count and maximum output
   length. It is admitted only when a slot is free and its prompt blocks fit.
   The queue has a hard limit; beyond it the proxy returns 429.
3. **Continuous batching.** `llama-server` batches active slots. The proxy keeps
   every slot fed and reads the batch effect from llama-server's counters.
4. **Preemption.** When a grow fails, the scheduler picks a victim and either
   recomputes it later or swaps it (simulator only). The real proxy recomputes
   because llama.cpp has no portable per-sequence KV swap over HTTP.

## Limits

- One GPU, one model. The simulator is calibrated to this card.
- The real proxy's block accounting is logical. llama.cpp owns the physical KV
  cache; the proxy enforces a smaller budget so eviction is forced. This is
  admission control, not a paged-attention kernel.
- Streaming requests are only preempted before their first token, because they
  cannot be restarted without duplicate output.
- The simulator is for policy comparison under one calibration, not a
  substitute for the real run.

## Layout

```text
cmd/serve       HTTP proxy with queue, slots, block budget, preemption
cmd/sim         discrete-event policy simulator
cmd/loadgen     open-loop load generator
internal/kv     block allocator and stats
internal/sched  admission, preemption, fairness, simulator
internal/proxy  real proxy and upstream client
configs/        calibration and deployment configs
bench/          calibration and real-run scripts
results/        committed JSON artifacts
```

## License

MIT.
