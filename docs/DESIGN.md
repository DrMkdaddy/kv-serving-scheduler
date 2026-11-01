# Design

This is a scheduler for one 16 GB GPU. It does not try to prove scale. It
tries to make the scheduling decisions visible and measure what happens when
the KV budget is smaller than the offered load.

## The budget

A sequence of `T` tokens needs `ceil(T / B)` blocks, where `B` is the block
size. The pool has `N` blocks. The proxy reserves the worst-case footprint at
admission:

```
reserved = prompt_tokens + max_tokens
blocks   = ceil(reserved / B)
```

Reserving the worst case is conservative. It means a request cannot fail
halfway through because the KV cache grew; it is either admitted or it waits.
The alternative is to allocate on demand and preempt on growth. The simulator
models that alternative because it is the more interesting policy question.

The real proxy runs llama-server with a larger physical cache (`-c` and `-np`)
and enforces a smaller logical pool. The pool is the admission model; llama.cpp
still owns the physical allocation. This distinction is stated in the README
and in the writeup because it is the difference between admission control and
a paged-attention kernel.

## Policies

Admission order:

- **FCFS**: earliest arrival first.
- **Priority**: lower `priority` value first, then arrival.
- **WFQ**: smallest virtual finish time. The virtual finish is
  `max(arrival, tenant_vtime) + (prompt + max_tokens) / weight`.

Preemption:

- **recompute**: cancel the victim, free its blocks, requeue it. On re-admission
  it re-prefills `prompt + generated` tokens. The real proxy uses this for
  non-streaming requests because llama.cpp does not expose a portable
  per-sequence GPU-to-host KV swap over HTTP.
- **swap**: free the victim's GPU blocks, keep its state, and add a restore
  cost when it comes back. The simulator models this; the real proxy does not
  implement it.
- **none**: shed the request instead of preempting. The simulator counts the
  failure in goodput; the real proxy returns an error.

Victim selection: `lowest_priority`, `largest`, `newest`, `oldest`, or
`tenant_share`. The real proxy refuses to evict a resident request for a newer
arrival under FCFS; that would be thrashing, not scheduling.

## Metrics

- **TTFT**: arrival to first generated token, including queue wait and prefill.
- **ITL**: time between consecutive generated tokens for one request.
- **Goodput**: fraction of *all offered requests* that meet both the TTFT and
  ITL SLO. Failed and shed requests are not goodput.
- **Jain**: `(sum x)^2 / (n * sum x^2)` over completed tokens per tenant. Raw
  token Jain is not meaningful when tenants offer different demand, so the
  summary also reports per-tenant completion ratios and p99 latencies.
- **KV blocks**: used, free, internal waste (`allocated_tokens - used_tokens`),
  and the number of free runs.

## The simulator

`cmd/sim` is a discrete-event model. It consumes a measured step-time curve:

```
step_ms(n) = interpolation of the measured (batch, step) points
prefill_ms = prompt_tokens / prefill_tps
```

At each step every ready sequence advances one token. The step costs
`step_ms(active)`. New arrivals are processed at step boundaries. Prefill is a
per-request latency, not a shared batch resource; this is an explicit
simplification. It matters for TTFT at high concurrency, and the real proxy
measurements are the check on it.

The simulator is used for policy sweeps that the real server cannot expose:
swap vs recompute, victim selection, and per-tenant fairness under a fixed
workload. Its absolute throughput is an upper bound because the calibration is
the direct llama-server path, while the real proxy adds a per-token SSE path to
measure TTFT and ITL.

## What would falsify the result

- If the real proxy produced the same TTFT curve at 3x the offered load, the
  admission model would be wrong.
- If preemptions did not increase under a smaller block pool, the allocator or
  the pressure workload would be wrong.
- If the high-priority tenant's p99 did not improve when priority was enabled,
  the queue policy would be wrong.
- If the generated outputs changed under preemption/recompute, the recompute
  path would be wrong. The real proxy returns a complete response only after
  the final attempt; the spec lab separately asserts bit-identical greedy
  outputs for its own changes.

## Threats to validity

- One GPU, one model pair, one host. The calibration is host-specific.
- The proxy's streaming path costs throughput. The measured 219 tok/s through
  the proxy is lower than the 358 tok/s direct path at three resident
  sequences; the gap is the observable per-token path, not a claim that the
  scheduler is free.
- The workload is synthetic: exponential arrivals, uniform prompt/output
  ranges. Real traffic has bursts and prefix reuse.
- The 6-slot calibration point was noisy under host CPU load; the simulator
  uses the first clean run and never extrapolates far beyond 4 slots.
