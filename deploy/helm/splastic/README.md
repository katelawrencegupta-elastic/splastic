# Splash Helm Chart

Minimal Kubernetes deploy: shared **classify** Deployment + **pipeline**
StatefulSet (one writer pod = one shard; cooked + uncooked; per-pod spill PVC)
behind a LoadBalancer Service VIP.

## Install

```bash
helm upgrade --install splash ./deploy/helm/splastic \
  --namespace splash --create-namespace \
  --set elastic.host="$ELASTIC_HOST" \
  --set elastic.apiKey="$ELASTIC_API_KEY" \
  --set classify.authToken="$CLASSIFY_AUTH_TOKEN" \
  --set pipeline.replicaCount=4 \
  --set classify.replicaCount=2
```

`elastic.apiKey` and `classify.authToken` are stored in a chart-managed Secret
and mounted via `secretKeyRef`. To use an externally managed Secret:

```bash
--set existingSecret=my-splash-creds
```

Point Splunk `tcpout` at the pipeline Service EXTERNAL-IP / hostname for cooked
port 39998 (see chart NOTES after install). Prefer VIP-only config so shard
add/remove does not churn forwarders.

## Capacity / multi-writer

| Unit | Throughput |
|------|------------|
| 1 writer pod | ~**0.008 GB/s** (~5k eps @ 1536 B) |
| N pods | ~N × 0.008 GB/s |

```text
shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)
```

| Daily volume (peak_factor=2) | `pipeline.replicaCount` | Fleet vCPU @ 8/node |
|-----------------------------|------------------------:|--------------------:|
| 1 TB/day | 4 | 32 |
| 5 TB/day | 19 | 152 |
| 10 TB/day | 37 | 296 |

Keep `classify.replicaCount` at **2–3** (shared ensure path only). Do not scale
classify 1:1 with writers. Pack **1 writer pod per 8-vCPU / 16 GiB node**.

Full strategy: [`docs/runbooks/sharding.md`](../../../docs/runbooks/sharding.md).

CPU requests/limits (defaults):

| Container | Requests | Limits |
|-----------|----------|--------|
| classify | 100m / 256Mi | 1 / 512Mi |
| writer | 500m / 512Mi | 4 / 2Gi |

Pods use `terminationGracePeriodSeconds: 40` (covers bulk drain). StatefulSet
rolling updates use `maxUnavailable: 1` to limit ensure-cache flush storms.
Writer metrics are labeled `shard="<pod-name>"` via `SPLASH_SHARD_ID` /
`POD_NAME`. Default `writerProcesses: "4"` runs SO_REUSEPORT workers inside
each pod (`WRITER_PROCESSES`); set to `"1"` for single-process debug.

Failed bulk docs land on the per-pod spill PVC (`WRITER_SPILL_DIR`); see
[`docs/runbooks/spill.md`](../../../docs/runbooks/spill.md).
