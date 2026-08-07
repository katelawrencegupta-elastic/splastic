# Compute optimization playbook — Splastic

Raise GB/s per vCPU-dollar with gated phases.

The saturator is the Python writer (bulk queue) and/or Elasticsearch. Re-baseline
Phase 0 with S1 before changing Helm CPU limits.

## Phase 0 — Baseline

```bash
./scripts/compute-optimize-baseline.sh
python -m loadtest run -s S1 --eps 5000 --duration 120
```

| Observation | Next |
|-------------|------|
| Queue pegs, classify idle | Scale writer CPU or shards; check ES |
| Cold-path / ensure high (`splastic:miss_fraction:1m`) | Phase 1 (metadata hit rate) |
| Otherwise | Horizontal shards (Phase 3) |

## Phase 1 — Metadata hit rate

Keep `miss_fraction < 0.1` via [`sidecar/classify_rules.json`](../../sidecar/classify_rules.json)
(synced to `packages/splastic-writer/writer/classify_rules.json`).

Miss = writer `classify_message_hit` + `classify_generic` (see recording rules in
[`deploy/alerts/splastic-recording.yaml`](../../deploy/alerts/splastic-recording.yaml)).

## Phase 2 — Multi-process writer (per-pod GB/s)

Default `WRITER_PROCESSES=4` (SO_REUSEPORT) so the Helm **4 CPU** limit is
usable. Set `WRITER_PROCESSES=1` to revert to a single asyncio process.

```bash
WRITER_PROCESSES=4 python -m loadtest run -s S1 --eps 10000 --duration 120
```

| Observation | Next |
|-------------|------|
| ≥~1.5× floor vs 0.008 (queue not pegged) | Re-lock `PLAN_GBPS`; fewer shards |
| Little gain (&lt;20%) | Profile hot path before raising CPU further |
| Queue pegs, CPU idle | Check ES / bulk RTT |

Then optionally probe writer CPU limits above 4 only with S1 ramp evidence.

## Phase 3 — Horizontal multi-writer shards

When ES is scaled and a single writer is at the ~0.008 GB/s floor, scale out:

1. Size: `shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)` (`suggest_shards.py`).
2. Deploy: Helm `pipeline.replicaCount=N` (shared classify 2–3) **or** local
   `./scripts/run-shard.sh` for soaks.
3. Front door: L4 VIP for cooked/uncooked; Splunk points at VIP only.
4. Prove: unit S1 → `S1x2` → N×5k eps soak (≥30 min).

Full topology, soak gates, packing (1 writer / 8-vCPU node), and skew handling:
[sharding.md](sharding.md).

## Phase 5 — Economics

Scale path remains horizontal writer shards from measured peak GB/s.
See [sharding.md](sharding.md) for 1 / 5 / 10 TB fleet vCPU tables.
