#!/usr/bin/env python3
"""Measure llama-server decode step time at several batch sizes.

Run against an already-running server. The output is the calibration block the
simulator consumes; it is a measurement, not a guess.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path


PROMPT = (
    "Explain how a modern CPU pipeline works, covering fetch, decode, "
    "out-of-order execution, register renaming, branch prediction, and the "
    "memory hierarchy. Write several paragraphs."
) * 2


def metrics(base: str) -> dict[str, float]:
    text = urllib.request.urlopen(base + "/metrics", timeout=10).read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        try:
            out[key] = float(value)
        except ValueError:
            pass
    return out


def one(base: str, index: int, barrier: threading.Barrier, results: list) -> None:
    body = json.dumps({
        "prompt": PROMPT,
        "n_predict": 128,
        "temperature": 0,
        "top_k": 1,
        "seed": 300 + index,
        "cache_prompt": False,
        "stream": False,
        "ignore_eos": True,
    }).encode()
    req = urllib.request.Request(
        base + "/completion", data=body, headers={"Content-Type": "application/json"}
    )
    barrier.wait()
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=300) as response:
        payload = json.loads(response.read())
    results[index] = (time.perf_counter() - started, payload.get("timings", {}))


def vram() -> int | None:
    try:
        out = subprocess.run(
            ["rocm-smi", "--showmeminfo", "vram", "--json"],
            capture_output=True, text=True, check=False,
        ).stdout
        data = json.loads(out)
        for card in data.values():
            for key, value in card.items():
                if "Used Memory" in key and "B" in key:
                    return int(value)
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8093")
    parser.add_argument("--out", default="results/calibration.json")
    parser.add_argument("--points", default="1,2,3,4,6,8")
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()
    points = [int(x) for x in args.points.split(",") if x]
    rows = []
    for n in points:
        reps = []
        for _ in range(args.reps):
            before = metrics(args.url)
            barrier = threading.Barrier(n)
            results: list = [None] * n
            threads = [
                threading.Thread(target=one, args=(args.url, i, barrier, results))
                for i in range(n)
            ]
            started = time.perf_counter()
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            wall = time.perf_counter() - started
            after = metrics(args.url)
            steps = after.get("llamacpp:n_decode_total", 0) - before.get("llamacpp:n_decode_total", 0)
            tokens_s = after.get("llamacpp:tokens_predicted_seconds_total", 0) - before.get(
                "llamacpp:tokens_predicted_seconds_total", 0
            )
            predicted = [r[1].get("predicted_per_second", 0) for r in results if r]
            prompt = [r[1].get("prompt_per_second", 0) for r in results if r]
            if steps and tokens_s:
                step_ms = 1000 * tokens_s / (n * steps)
            else:
                per_token = [
                    r[1]["predicted_ms"] / r[1]["predicted_n"]
                    for r in results
                    if r and r[1].get("predicted_n")
                ]
                step_ms = statistics.mean(per_token) if per_token else 0
            reps.append({
                "n": n,
                "step_ms": step_ms,
                "aggregate_tps": sum(predicted),
                "prefill_tps": statistics.mean(prompt) if prompt else None,
                "wall_s": wall,
            })
        med = {
            "n": n,
            "step_ms": round(statistics.median(r["step_ms"] for r in reps), 2),
            "aggregate_tps": round(statistics.median(r["aggregate_tps"] for r in reps), 1),
            "prefill_tps": round(statistics.median(r["prefill_tps"] for r in reps), 1)
            if all(r["prefill_tps"] for r in reps) else None,
        }
        rows.append(med)
        print(json.dumps(med))

    artifact = {
        "schema": "kv-scheduler/calibration/v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "url": args.url,
        "points": rows,
        "vram_loaded_bytes": vram(),
        "method": "n concurrent greedy /completion requests, 128 tokens, cache_prompt=false; step_ms = 1000 * tokens_predicted_seconds_total / (n * n_decode_total); median of reps.",
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(artifact, indent=2) + "\n")
    print("wrote", args.out)


if __name__ == "__main__":
    main()
