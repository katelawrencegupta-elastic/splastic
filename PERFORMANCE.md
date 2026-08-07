# Splash Performance Analysis

## Architecture

```
Splunk cooked tcpout :39998
        │
        ▼
  splastic-writer                 Splunk uncooked :39997
  (s2s decode + classify)                  │
        │                                  │
        └────────────────┬─────────────────┘
                         ▼
              normalize + in-process classify
                         │
           first-seen stream → POST /ensure/batch
                         │
                         ▼
              splash-classify :8080
              (index template + /ensure/batch)
                         │
                         ▼
              Elasticsearch _bulk (create)
```

**Steady-state HTTP profile:** metadata-rich Splunk traffic → ensure HTTP only on
newly seen data streams. Classify sidecar stays at `UVICORN_WORKERS=1`.

---

## Compute baseline

Measured local Docker → Elastic Cloud POC (Aug 2026):

| Item | Value |
|------|-------|
| S1 @ 5k eps / 1536 B (1 or 4 procs) | **passed** — `avg_gbps≈0.0083` |
| Planning floor | **0.008 GB/s / writer stack** |
| `WRITER_PROCESSES=4` ramp | 7.5k–15k eps **queue-pegged** vs remote ES; no ≥1.5× floor lock |
| Scale path | Horizontal writer shards; size ES so bulk keeps up |

Multi-process (`SO_REUSEPORT`) is on by default so pod CPU can be used when ES
is co-located / scaled. Re-lock `PLAN_GBPS` only after a clean S1 pass above
~0.012 GB/s (see [compute-optimize](docs/runbooks/compute-optimize.md) Phase 2).

```bash
WRITER_PROCESSES=1 python -m loadtest run -s S1 --eps 5000 --duration 120
WRITER_PROCESSES=4 python -m loadtest run -s S1 --eps 10000 --duration 120
```

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| 1 | GB/s floor with S1 | High | Done — **0.008 GB/s/stack** |
| 2 | Metadata hit rate | High ($/GB) | Rules synced writer ↔ sidecar |
| 3 | Spill / bulk-fail alerting | Med | `splash_writer_indexed_fail_total` |

## Smoke checklist

- Event with `sourcetype=access_combined` → local classify; one `/ensure/batch` then zero ensure HTTP
- Same stream again → zero ensure HTTP
- Empty sourcetype/source + access log `message` → message-pattern classify in writer
- Uncooked TCP `:39997` accepts plain lines and JSON objects
- Failed bulk → rows in `WRITER_SPILL_DIR` / `spill.ndjson`
