#!/usr/bin/env bash
# Reproduce the simulator, calibration, and (optionally) the real proxy runs.
set -euo pipefail

cd "$(dirname "$0")/.."

go test ./...
go run ./cmd/sim --config configs/qwen3-4b-calibrated.json --out results/sim_$(date -u +%Y%m%dT%H%M%SZ).json

if [ "${REAL:-0}" = "1" ]; then
  ./bench/calibrate.sh
  ./bench/real.sh
else
  echo "skipping GPU runs; set REAL=1 to run calibration and the proxy load tests"
fi
