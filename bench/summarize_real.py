#!/usr/bin/env python3
"""Summarize the loadgen artifacts into one table."""

from __future__ import annotations

import glob
import json
from pathlib import Path


def load(pattern: str) -> list[dict]:
    return [json.loads(Path(p).read_text()) for p in sorted(glob.glob(pattern))]


def row(data: dict) -> dict:
    server = data.get("server", {})
    metrics = server.get("metrics", {})
    tokens = metrics.get("tokens", 0)
    uptime = metrics.get("uptime_seconds", data.get("duration_s", 1)) or 1
    return {
        "offered_rate": data["rate"],
        "requests": data["requests"],
        "completed": data["completed"],
        "errors": data["errors"],
        "throughput_tps": round(tokens / uptime, 1),
        "wall_p50_ms": round(data["wall_p50_ms"], 1),
        "wall_p99_ms": round(data["wall_p99_ms"], 1),
        "ttft_p50_ms": round(data["ttft_p50_ms"], 1),
        "ttft_p99_ms": round(data["ttft_p99_ms"], 1),
        "queue_p99_ms": round(data["queue_p99_ms"], 1),
        "preemptions": data.get("preemptions", 0),
        "source": data.get("generated_at", ""),
    }


def main() -> None:
    rates = load("results/real_rate*.json")
    mixed_low = load("results/real_mixed_low.json")
    mixed_high = load("results/real_mixed_high.json")
    artifact = {
        "schema": "kv-scheduler/real/v1",
        "rates": [row(r) for r in rates],
        "mixed": {
            "low": row(mixed_low[0]) if mixed_low else None,
            "high": row(mixed_high[0]) if mixed_high else None,
            "tenants": (mixed_low[0].get("server", {}).get("metrics", {}).get("tenants", {})
                        if mixed_low else {}),
        },
    }
    out = Path("results/real_summary.json")
    out.write_text(json.dumps(artifact, indent=2) + "\n")

    lines = ["| Offered req/s | Completed | Errors | Tok/s | TTFT p50 | TTFT p99 | Wall p99 | Preemptions |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in artifact["rates"]:
        lines.append(
            f"| {r['offered_rate']:.0f} | {r['completed']}/{r['requests']} | {r['errors']} | "
            f"{r['throughput_tps']:.0f} | {r['ttft_p50_ms']:.0f} ms | {r['ttft_p99_ms']:.0f} ms | "
            f"{r['wall_p99_ms']:.0f} ms | {r['preemptions']} |"
        )
    lines.append("")
    lines.append("| Mixed-priority tenant | Requests | TTFT p99 | Queue p99 | Preemptions |")
    lines.append("|---|---:|---:|---:|---:|")
    for name, view in artifact["mixed"].get("tenants", {}).items():
        lines.append(
            f"| {name} | {view.get('requests', 0)} | {view.get('ttft_p99_ms', 0):.0f} ms | "
            f"{view.get('queue_p99_ms', 0):.0f} ms | {view.get('preempted', 0)} |"
        )
    Path("results/real_summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print("wrote", out)


if __name__ == "__main__":
    main()
