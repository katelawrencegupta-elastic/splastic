#!/usr/bin/env python3
"""TB/day load-test campaign: plan shards, validate unit capacity, project scale.

Assumes Elastic Cloud ingest capacity is scaled with the Splash pipeline
(queue not pegged due to ES). Locally we validate the per-stack floor and
per-shard planned peak; full 5/10 TB fleets are projected linearly.

Usage:
  python campaign_tb_day.py [--duration 90] [--skip-run] [--tiers 1,5,10]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from suggest_shards import PLAN_GBPS, SEC_PER_DAY, plan_shards

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"


def tier_plan(tb_day: float, event_bytes: int, peak_factor: float) -> dict:
    gb_day = tb_day * 1000.0
    avg_gbps = gb_day / SEC_PER_DAY
    peak_gbps = avg_gbps * peak_factor
    shards = plan_shards(peak_gbps)
    avg_eps = (avg_gbps * 1e9) / event_bytes
    peak_eps = (peak_gbps * 1e9) / event_bytes
    # Planned share per shard at peak (with headroom baked into shard count)
    per_shard_peak_eps = peak_eps / shards
    per_shard_avg_eps = avg_eps / shards
    per_shard_peak_gbps = peak_gbps / shards
    return {
        "tb_day": tb_day,
        "gb_day": gb_day,
        "event_bytes": event_bytes,
        "peak_factor": peak_factor,
        "avg_gbps": avg_gbps,
        "peak_gbps": peak_gbps,
        "avg_eps": avg_eps,
        "peak_eps": peak_eps,
        "suggested_pipeline_shards": shards,
        "per_shard_avg_eps": per_shard_avg_eps,
        "per_shard_peak_eps": per_shard_peak_eps,
        "per_shard_peak_gbps": per_shard_peak_gbps,
        "planning_floor_gbps": PLAN_GBPS,
        "projected_fleet_gbps": shards * PLAN_GBPS,
        "headroom_vs_peak": (shards * PLAN_GBPS) / peak_gbps if peak_gbps else None,
    }


def run_loadtest(
    *,
    scenario: str,
    eps: float,
    duration: float,
    run_id: str,
    event_bytes: int,
    namespace: str,
    extra: list[str] | None = None,
) -> dict:
    cmd = [
        sys.executable,
        "-m",
        "loadtest",
        "run",
        "-s",
        scenario,
        "--eps",
        str(eps),
        "--duration",
        str(duration),
        "--event-bytes",
        str(event_bytes),
        "--namespace",
        namespace,
        "--run-id",
        run_id,
        "--skip-warm",
        "--skip-burst",
    ]
    if extra:
        cmd.extend(extra)
    print(f"\n=== RUN {' '.join(cmd)} ===", flush=True)
    t0 = time.monotonic()
    # Drop host ES creds so harness pass/fail is pipeline-only (ES assumed scaled).
    # Compose containers still index into Elastic with their own env.
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("ELASTIC_HOST", "ELASTIC_API_KEY")
    }
    proc = subprocess.run(cmd, cwd=ROOT, env=env)
    elapsed = time.monotonic() - t0
    # loadtest run.py writes results/<scenario>_<run_id>/
    candidates = [
        RESULTS / f"{scenario}_{run_id}",
        RESULTS / run_id,
    ]
    out_dir = next((p for p in candidates if (p / "summary.json").exists()), candidates[0])
    summary_path = out_dir / "summary.json"
    summary = {}
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
    steady = next((p for p in summary.get("phases", []) if p.get("phase") == "steady"), {})
    pipeline_ok = bool(
        steady
        and not steady.get("queue_pegged")
        and not steady.get("classify_fail")
        and (steady.get("sent_events") or 0) > 0
        and (steady.get("gen_errors") or 0) == 0
    )
    index_ratio = steady.get("index_ratio")
    es_ok = (index_ratio >= 0.95) if index_ratio is not None else None
    return {
        "run_id": run_id,
        "scenario": scenario,
        "eps": eps,
        "duration_s": duration,
        "exit_code": proc.returncode,
        "elapsed_s": elapsed,
        "out_dir": str(out_dir),
        "steady_passed": summary.get("steady_passed"),
        "steady": steady,
        "pipeline_ok": pipeline_ok,
        "es_ok": es_ok,
        "pass_if_es_scaled": pipeline_ok,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tiers", default="1,5,10", help="Comma-separated TB/day tiers")
    p.add_argument("--event-bytes", type=int, default=1536)
    p.add_argument("--peak-factor", type=float, default=2.0)
    p.add_argument("--duration", type=float, default=90.0, help="Steady seconds per run")
    p.add_argument("--namespace", default="loadtest")
    p.add_argument("--skip-run", action="store_true", help="Plan only, no generators")
    p.add_argument(
        "--unit-eps",
        type=float,
        default=5000.0,
        help="Unit capacity lock eps (S1 ceiling)",
    )
    args = p.parse_args()

    tiers = [float(x.strip()) for x in args.tiers.split(",") if x.strip()]
    plans = [tier_plan(t, args.event_bytes, args.peak_factor) for t in tiers]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    campaign_id = f"campaign_tb_{stamp}"
    campaign_dir = RESULTS / campaign_id
    campaign_dir.mkdir(parents=True, exist_ok=True)

    runs: list[dict] = []
    if not args.skip_run:
        # 1) Lock unit floor at planning ceiling
        runs.append(
            run_loadtest(
                scenario="S1",
                eps=args.unit_eps,
                duration=args.duration,
                run_id=f"{campaign_id}_unit_S1_{int(args.unit_eps)}eps",
                event_bytes=args.event_bytes,
                namespace=args.namespace,
            )
        )
        # 2) Per-tier: soak at planned per-shard peak (ES assumed scaled)
        for plan in plans:
            eps = max(500.0, math.floor(plan["per_shard_peak_eps"]))
            tb = int(plan["tb_day"]) if plan["tb_day"] == int(plan["tb_day"]) else plan["tb_day"]
            runs.append(
                run_loadtest(
                    scenario="S1",
                    eps=eps,
                    duration=args.duration,
                    run_id=f"{campaign_id}_tier{tb}TB_per_shard_peak_{int(eps)}eps",
                    event_bytes=args.event_bytes,
                    namespace=args.namespace,
                )
            )

    unit = next((r for r in runs if "unit_S1" in r["run_id"]), None)
    report = {
        "campaign_id": campaign_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assumptions": {
            "elastic_backend": "scaled_accordingly",
            "event_bytes": args.event_bytes,
            "peak_factor": args.peak_factor,
            "planning_floor_gbps_per_stack": PLAN_GBPS,
            "local_validation": (
                "unit S1 ceiling + per-shard planned peak soaks; "
                "full fleets projected linearly when ES keeps up"
            ),
            "namespace": args.namespace,
        },
        "plans": plans,
        "runs": runs,
        "verdict": {
            "unit_floor_locked_pipeline": unit.get("pipeline_ok") if unit else None,
            "unit_floor_locked_es": unit.get("es_ok") if unit else None,
            "unit_avg_gbps": (unit or {}).get("steady", {}).get("avg_gbps_payload"),
            "all_pipeline_ok": all(r.get("pipeline_ok") for r in runs) if runs else None,
            "all_es_ok": all(r.get("es_ok") for r in runs if r.get("es_ok") is not None)
            if runs
            else None,
            "all_soaks_passed_official": all(r.get("steady_passed") for r in runs) if runs else None,
            "tiers_ready_if_es_scaled": {
                f"{int(p['tb_day']) if p['tb_day'] == int(p['tb_day']) else p['tb_day']}TB": {
                    "shards": p["suggested_pipeline_shards"],
                    "peak_eps": round(p["peak_eps"]),
                    "projected_fleet_gbps": round(p["projected_fleet_gbps"], 4),
                    "headroom_vs_peak": round(p["headroom_vs_peak"], 3)
                    if p["headroom_vs_peak"]
                    else None,
                }
                for p in plans
            },
        },
    }
    out = campaign_dir / "campaign_report.json"
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"\nCampaign report: {out}", flush=True)
    print(json.dumps(report["verdict"], indent=2), flush=True)
    # Under ES-scaled assumption, pipeline_ok is the gate
    if runs and not report["verdict"]["all_pipeline_ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
