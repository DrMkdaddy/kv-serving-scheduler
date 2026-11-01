#!/usr/bin/env bash
# Start llama-server, measure the decode step curve, stop the server.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${LLAMA_BIN:-$HOME/llama.cpp/src-b11050/build/bin}"
MODEL="${MODEL:-$HOME/models/Qwen3-4B-Instruct-2507-Q4_K_M.gguf}"
PORT="${PORT:-8093}"
CTX="${CTX:-16384}"
NP="${NP:-8}"

"$BIN/llama-server" \
  -m "$MODEL" --host 127.0.0.1 --port "$PORT" \
  -c "$CTX" -np "$NP" -ngl 99 -fa on -ctk f16 -ctv f16 --metrics \
  > "${TMPDIR:-/tmp}/kv-calibrate-server.log" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null || true' EXIT

for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null; then break; fi
  sleep 0.5
done
curl -sf "http://127.0.0.1:$PORT/health" >/dev/null || {
  echo "server did not become ready; see ${TMPDIR:-/tmp}/kv-calibrate-server.log" >&2
  exit 1
}

python3 "$ROOT/bench/calibrate.py" --url "http://127.0.0.1:$PORT" --out "$ROOT/results/calibration.json"
