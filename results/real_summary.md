| Offered req/s | Completed | Errors | Tok/s | TTFT p50 | TTFT p99 | Wall p99 | Preemptions |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 19/19 | 0 | 122 | 53 ms | 55 ms | 949 ms | 0 |
| 2 | 39/39 | 0 | 210 | 1480 ms | 3115 ms | 4562 ms | 0 |
| 3 | 59/59 | 0 | 219 | 6301 ms | 8010 ms | 9602 ms | 0 |
| 4 | 79/79 | 0 | 218 | 7333 ms | 8866 ms | 10318 ms | 0 |

| Mixed-priority tenant | Requests | TTFT p99 | Queue p99 | Preemptions |
|---|---:|---:|---:|---:|
| high | 14 | 74 ms | 3 ms | 0 |
| low | 89 | 12212 ms | 12158 ms | 14 |
