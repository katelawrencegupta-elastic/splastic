# Alerting runbook — Splastic

Metrics sources:

| Component | Path | Port |
|-----------|------|------|
| s2s-decode / writer | `/metrics` | 8081 (or shard offset) |
| classify | `/metrics` | 8080 |
| index_lag_probe | `/metrics` | 9103 |

Rules: [deploy/alerts/splastic-alerts.yaml](../../deploy/alerts/splastic-alerts.yaml),
recording: [deploy/alerts/splastic-recording.yaml](../../deploy/alerts/splastic-recording.yaml).

## SplasticS2SQueuePegged

**Meaning:** writer upstream queue &gt; 9000 for 2m (capacity 10000).

**Actions:**
1. Check ES bulk latency and writer CPU (`WRITER_PROCESSES`, pod limits).
2. Scale out another shard (see [sharding.md](sharding.md)).
3. Confirm classify is not in a cold-path storm (`splastic:miss_fraction:1m`).
4. If failures are accumulating, check spill ([spill.md](spill.md)).

## SplasticClassifyNotReady

**Meaning:** `splastic_classify_ready == 0` (index template not ensured).

**Actions:**
1. `curl classify:8080/health` — read `reason` (no cluster URL is returned).
2. Verify ES credentials, network, and that classify can PUT the index template.
3. Check classify logs for template ensure errors; restart after ES is reachable.

## SplasticWriterBulkFailures

**Meaning:** `splastic_writer_indexed_fail_total` increased over a 15m window.

**Actions:** Follow [spill.md](spill.md) — identify cause, fix ES/mappings, replay or drop spill.

## SplasticIndexLagHigh

**Meaning:** `splastic_index_lag_seconds` &gt; 60 for 10m (index lag probe).

**Actions:**
1. Confirm probe targets and ES `_count` access.
2. Check writer bulk latency / queue peg / ES ingest pressure.
3. Temporarily lower offered eps or scale writer shards.

## SplasticPeakToAvgHigh

**Meaning:** `splastic:peak_to_avg:1d` &gt; 3.

**Actions:**
1. Query `max_over_time(splastic:ingest_gbps:5m[1d])` — that is peak GB/s.
2. Re-size: `shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)` (see [sharding.md](sharding.md)).
3. Cross-check VIP/NLB ProcessedBytes for the same window.
4. Do not size from daily TB totals alone.

## SplasticClassifyMissStorm

**Meaning:** metadata-miss path dominates — `miss_fraction` &gt; 0.25 or miss eps &gt; 2k for 5m while ingest is non-trivial.

Recording rules derive miss from writer counters:
`splastic_writer_classify_message_hit_total` + `splastic_writer_classify_generic_total`
(events that did not match metadata `sourcetype`/`source` rules).

**Actions:**
1. Inspect `splastic:classify_miss_eps:1m` vs `splastic:ingest_eps:1m`, and
   `rate(splastic_writer_classify_meta_hit_total[5m])`.
2. Expand [`sidecar/classify_rules.json`](../../sidecar/classify_rules.json) (keep
   [`packages/splastic-writer/writer/classify_rules.json`](../../packages/splastic-writer/writer/classify_rules.json) in sync).
3. Check Splunk UF/HF that `sourcetype` / `source` are populated (empty → message path).
4. Re-run loadtest `S2` (cold) / `S3` (mixed) after rule changes.

## Scrape + remote_write

Config: [`deploy/prometheus/prometheus.yml`](../../deploy/prometheus/prometheus.yml).

```bash
docker compose --profile metrics up -d --build
```

Prometheus scrapes `s2s-decode:8081`, `classify:8080`, and `index-lag-probe:9103`,
then **remote_writes** to:

`${ELASTIC_HOST}/_prometheus/api/v1/write`

(same cluster as ingest). Override with `PROMETHEUS_REMOTE_WRITE_URL` if needed.

Auth: prefer `PROMETHEUS_ELASTIC_API_KEY` with **metrics-*** privileges; falls back to `ELASTIC_API_KEY` (logs-only keys often get 403 on remote_write). Whitespace-only host/URL vars fail startup.

Multi-shard: `SPLASTIC_SHARD_ID` becomes `external_labels.splastic_shard`; host ports offset via `run-shard.sh`. UI on loopback `:9090` (shard 0).

```yaml
scrape_configs:
  - job_name: splastic-s2s
    static_configs:
      - targets: ["s2s-decode:8081"]
  - job_name: splastic-classify
    static_configs:
      - targets: ["classify:8080"]
  - job_name: splastic-index-lag
    static_configs:
      - targets: ["index-lag-probe:9103"]

remote_write:
  - url: ${ELASTIC_HOST}/_prometheus/api/v1/write   # rendered at container start
```

## Security note

mTLS / ingest auth is a separate track (not covered by these alerts).
