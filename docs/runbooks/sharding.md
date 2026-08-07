# Sharding and VIP — Splash multi-writer strategy

Horizontal scale unit: **one writer pod = one shard**. Shared classify stays
small; Elasticsearch is assumed scaled so `_bulk` does not peg the writer queue.

```text
Splunk tcpout → L4 VIP :39998/:39997 → writer_0 … writer_N → ES _bulk
                                      ↘ ensure (cold only) → classify
```

## Locked topology

| Piece | Choice |
|-------|--------|
| Scale unit | 1 writer pod (~**0.008 GB/s**, ~5k eps @ 1536 B) |
| Classify | **Shared** Deployment (2–3 replicas); not 1:1 with writers |
| Front door | Single **L4 TCP VIP** (round-robin / least-conn) |
| Packing | **1 writer / 8-vCPU / 16 GiB node** |
| Stickiness | Not required; long Splunk TCP sessions pin until reconnect |

## Capacity

| Unit | Sustainable throughput |
|------|------------------------|
| 1 writer shard | **~0.008 GB/s** (S1 @ 5k eps, Aug 2026; re-lock after `WRITER_PROCESSES>1`) |
| N shards | **~N × floor GB/s** |

Per-pod multi-process (`WRITER_PROCESSES=4`, SO_REUSEPORT) may raise the unit
floor; only change `PLAN_GBPS` / shard tables after a clean S1 ramp. See
[compute-optimize.md](compute-optimize.md) Phase 2.

```text
peak_GBps = (TB_day * 1000 / 86400) * peak_factor
shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)
```

| Daily volume | Shards (peak_factor=2, 1536 B) | Fleet vCPU @ 8/node |
|--------------|-------------------------------:|--------------------:|
| 1 TB/day | 4 | 32 |
| 5 TB/day | 19 | 152 |
| 10 TB/day | 37 | 296 |

```bash
python suggest_shards.py --tb-day 1 --event-bytes 1536 --peak-factor 2
```

Set Helm `pipeline.replicaCount` to the suggested shard count. Keep
`classify.replicaCount` at 2–3 unless ensure storms show load.

Do not plan above ~0.015 GB/s per shard without a fresh ramp test.

### Measuring peak vs average

Prometheus recording rules ([`deploy/alerts/splash-recording.yaml`](../../deploy/alerts/splash-recording.yaml)):

| Series | Meaning |
|--------|---------|
| `splash:ingest_gbps:5m` | Ingest GB/s from s2s `bytes_consumed` |
| `splash:ingest_eps:1m` | Events/s from s2s `events_emitted` |
| `splash:peak_to_avg:1d` | max(5m GB/s over 1d) / avg(5m GB/s over 1d) |
| `splash_s2s_avg_event_bytes` | Lifetime bytes/event (size skew signal) |

**Workflow:**

1. After ≥24h of production, query `splash:peak_to_avg:1d` and
   `max_over_time(splash:ingest_gbps:5m[1d])`.
2. Plug peak into the shard formula above.
3. Cross-check cloud NLB/VIP **ProcessedBytes** on the cooked listener.
4. If `splash_s2s_avg_event_bytes` is far from ~1536, re-run `S1_512` /
   `S1_1536` / `S1_4096` before locking shard count.
5. Alert `SplashPeakToAvgHigh` fires when peak/avg &gt; 3.

Writer metrics include a `shard` label when `SPLASH_SHARD_ID` is set — use
`sum by (shard) (splash_s2s_upstream_queue)` to spot skew.

### Event size

Capacity assumes ~1.5 KB events. Smaller events raise CPU per GB. Measure P50
with `splash_s2s_avg_event_bytes` (or Splunk `_raw` length) and re-measure if
≠ ~1.5 KB.

## Kubernetes (production)

Chart: [`deploy/helm/splastic`](../../deploy/helm/splastic).

- Writer **StatefulSet** replicas = shard count (each pod: cooked + uncooked +
  health `:8081`, spill PVC).
- Shared **classify** Deployment behind ClusterIP.
- Pipeline **LoadBalancer** Service is the in-cluster VIP (`cookedPort` /
  `uncookedPort`). For cloud NLB in front of nodes, target the same ports and
  health-check `GET /health` on 8081 (or TCP).

```bash
helm upgrade --install splash ./deploy/helm/splastic \
  --namespace splash --create-namespace \
  --set elastic.host="$ELASTIC_HOST" \
  --set elastic.apiKey="$ELASTIC_API_KEY" \
  --set classify.authToken="$CLASSIFY_AUTH_TOKEN" \
  --set pipeline.replicaCount=4 \
  --set classify.replicaCount=2
```

Rolling updates use `maxUnavailable: 1` so ensure caches do not flush on every
pod at once (ensure set is **per-writer**, in-memory).

Point Splunk `tcpout` at the Service EXTERNAL-IP / hostname only — see chart
NOTES after install.

## Local compose shards

```bash
./scripts/run-shard.sh 0 up --build -d   # :39997 / :39998
./scripts/run-shard.sh 1 up --build -d   # :40007 / :40008
```

See [`docker-compose.shard.yml`](../../docker-compose.shard.yml). Each local
shard runs its **own** classify (compose isolation). That is fine for writer
path soaks; production still uses shared classify.

Generate loadtest targets for N shards (port stride 10):

```bash
python shard_targets.py --shards 4
# cooked_ports=39998,40008,40018,40028
# s2s_health_urls=http://127.0.0.1:8081/health,...
```

## VIP / L4 load balancer

Pass-through TCP; do not terminate TLS unless Splunk is terminated separately.
No session stickiness required for capacity.

### Backends

- Cooked: `writerN:39998`
- Uncooked: `writerN:39997`
- Health: HTTP `GET http://writerN:8081/health` or TCP connect

### HAProxy example

```text
listen splash_cooked
  bind *:39998
  mode tcp
  balance roundrobin
  option tcp-check
  server s0 10.0.1.10:39998 check
  server s1 10.0.1.11:39998 check

listen splash_uncooked
  bind *:39997
  mode tcp
  balance roundrobin
  option tcp-check
  server s0 10.0.1.10:39997 check
  server s1 10.0.1.11:39997 check
```

### Cloud NLB

TCP Network Load Balancer → target group of Splash nodes on 39998 / 39997.
Prefer HTTP health on 8081 when supported. Use NLB **ProcessedBytes** as an
independent peak/avg check against `splash:ingest_gbps:*`.

### Splunk

Point `tcpout` `server` at the VIP hostname. Prefer multiple connections /
autoLB so reconnects redistribute after scale-out. See
[`splunk/outputs.conf`](../../splunk/outputs.conf).

## Failure / cold-start

- **Ensure cache is per-writer.** Full-fleet restart causes a short parallel
  ensure burst; `/ensure/batch` is idempotent. Prefer rolling updates.
- **Spill** is per-pod PVC — alert on `splash_writer_spill_writes_total` and
  `splash_writer_indexed_fail_total`, not a shared DLQ.
- **Skew:** long TCP sessions can load one shard harder. If sustained
  `bytes_consumed` skew &gt; ~2×, force forwarder reconnects or drain the hot
  backend from the VIP briefly.

## Soak checklist (ES scaled)

1. **Unit:** `S1` @ 5k eps — queue not pegged (floor lock).
2. **Two-shard:** scenario `S1x2` (ports 39998+40008) @ 10k eps ≥30 min.
3. **Fleet:** N shards via `run-shard.sh` or Helm; loadtest at **N × 5k** eps
   ≥30 min:

   ```bash
   eval "$(python shard_targets.py --shards N --export)"
   python -m loadtest run -s S1 --eps $((N * 5000)) --duration 1800 \
     --cooked-ports "$COOKED_PORTS" --s2s-health-urls "$S2S_HEALTH_URLS"
   ```

4. **Pass:** per-shard `upstream_queue` p99 &lt; ~2k, classify `/health` 200,
   `indexed_fail` / spill flat, no queue peg at capacity for &gt;1 minute.
5. **Fail:** any shard queue pegged at 10k for &gt;1 minute, or rising
   `indexed_fail` / spill while ES is supposed to be scaled.

TB/day planning still uses [`loadtest/campaign_tb_day.py`](../../loadtest/campaign_tb_day.py)
(unit + per-shard peak soaks); full N-node fleets are projected until a real
multi-node soak.

## Observability

| Signal | Use |
|--------|-----|
| `splash_s2s_upstream_queue` (per `shard`) | Saturation / skew |
| `splash:ingest_gbps:5m` | Capacity / peak |
| `splash_writer_indexed_fail_total` / spill | Bulk / ES problems |
| `splash_writer_ensure_calls_total` | Cold-start / rule miss |
| NLB ProcessedBytes | Independent peak check |
