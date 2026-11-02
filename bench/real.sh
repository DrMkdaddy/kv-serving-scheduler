#!/usr/bin/env bash
# Run the real proxy load tests: throughput curve and mixed-priority preemption.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN="${LLAMA_BIN:-$HOME/llama.cpp/src-b11050/build/bin}"
MODEL="${MODEL:-$HOME/models/Qwen3-4B-Instruct-2507-Q4_K_M.gguf}"
UPSTREAM_PORT="${UPSTREAM_PORT:-8093}"
PROXY_PORT="${PROXY_PORT:-8080}"
PROMPT="$ROOT/bench/prompts/long.txt"

mkdir -p bin results
go build -o bin/serve ./cmd/serve
go build -o bin/loadgen ./cmd/loadgen

if ss -ltn 2>/dev/null | grep -q ":${UPSTREAM_PORT} "; then
  echo "port ${UPSTREAM_PORT} is already in use; stop the old server first" >&2
  exit 1
fi

"$BIN/llama-server" \
  -m "$MODEL" --host 127.0.0.1 --port "$UPSTREAM_PORT" \
  -c 16384 -np 8 -ngl 99 -fa on -ctk f16 -ctv f16 --metrics \
  > "${TMPDIR:-/tmp}/kv-real-server.log" 2>&1 &
SERVER=$!
PROXY_PID=""
cleanup() {
  if [ -n "$PROXY_PID" ]; then kill "$PROXY_PID" 2>/dev/null || true; fi
  kill "$SERVER" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:${UPSTREAM_PORT}/health" >/dev/null; then break; fi
  sleep 0.5
done
curl -sf "http://127.0.0.1:${UPSTREAM_PORT}/health" >/dev/null || {
  echo "llama-server did not become ready; see ${TMPDIR:-/tmp}/kv-real-server.log" >&2
  exit 1
}

restart_proxy() {
  local pid
  pid=$(ss -ltnp 2>/dev/null | sed -n "s/.*:${PROXY_PORT} .*pid=\\([0-9]*\\).*/\\1/p" | head -1)
  if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; sleep 1; fi
  ./bin/serve --config configs/qwen3-4b-real.json \
    --listen "127.0.0.1:${PROXY_PORT}" \
    --upstream "http://127.0.0.1:${UPSTREAM_PORT}" \
    > "${TMPDIR:-/tmp}/kv-proxy.log" 2>&1 &
  PROXY_PID=$!
  for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:${PROXY_PORT}/health" >/dev/null; then return; fi
    sleep 0.5
  done
  echo "proxy did not become ready" >&2
  exit 1
}

for rate in 1 2 3 4; do
  restart_proxy
  ./bin/loadgen --url "http://127.0.0.1:${PROXY_PORT}" \
    --rate "$rate" --duration 20s --concurrency 16 --max-tokens 128 \
    --prompt-file "$PROMPT" --tenant load --priority 0 \
    --out "results/real_rate${rate}.json"
done

restart_proxy
./bin/loadgen --url "http://127.0.0.1:${PROXY_PORT}" \
  --rate 3 --duration 30s --concurrency 16 --max-tokens 128 \
  --prompt-file "$PROMPT" --tenant low --priority 1 \
  --out results/real_mixed_low.json > /tmp/kv-mixed-low.log 2>&1 &
LOW=$!
./bin/loadgen --url "http://127.0.0.1:${PROXY_PORT}" \
  --rate 0.5 --duration 30s --concurrency 4 --max-tokens 128 \
  --prompt-file "$PROMPT" --tenant high --priority 0 \
  --out results/real_mixed_high.json > /tmp/kv-mixed-high.log 2>&1 &
HIGH=$!
wait "$LOW" "$HIGH"
cat /tmp/kv-mixed-low.log /tmp/kv-mixed-high.log

python3 bench/summarize_real.py
