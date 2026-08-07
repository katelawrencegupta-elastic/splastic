# Splash

Splunk → Elasticsearch ingest bridge. Terminates Splunk forwarder traffic (cooked
S2S and uncooked TCP), classifies events into ECS data streams, ensures those
streams exist, and indexes into Elastic as `logs-{dataset}-{namespace}`.

Repo directory: `splastic`. Product name in docs/compose/Helm: **Splash**.

## Architecture

```
Splunk cooked tcpout :39998          Splunk uncooked :39997
         │                                    │
         └────────────────┬───────────────────┘
                          ▼
              splastic-writer (Python)
              decode / normalize / classify
                          │
                first-seen stream only
                          │
                          ▼
                 splash-classify :8080
                 (template + /ensure/batch)
                          │
                          ▼
              Elasticsearch _bulk (create)
```

| Service | Role |
|---------|------|
| **s2s-decode / writer** | Cooked S2S `:39998` + uncooked TCP `:39997` → in-process classify → ES `_bulk` |
| **classify** | Index template readiness, `POST /ensure/batch` |

### Hybrid classify

Shared rules live in [`sidecar/classify_rules.json`](sidecar/classify_rules.json)
(keep in sync with
[`packages/splastic-writer/writer/classify_rules.json`](packages/splastic-writer/writer/classify_rules.json)).

- **Metadata hit** (`sourcetype` / `source` matches rules): classify in the writer.
  Call `POST /ensure/batch` only the first time a data stream is seen.
- **Metadata miss**: message-pattern classify in the writer (same rule set).

Steady-state Splunk traffic with known sourcetype/source pays almost no ensure HTTP.

Failed ES bulk docs spill to `WRITER_SPILL_DIR` (compose volume `writer_spill`).
See [`docs/runbooks/spill.md`](docs/runbooks/spill.md).

## Quick start

1. Create a `.env` in this directory (or symlink to a parent `.env`).

2. Required:

```bash
ELASTIC_HOST=https://your-cluster.es.region.cloud:443
ELASTIC_API_KEY=id:secret   # or base64 ApiKey
DATA_STREAM_NAMESPACE=default
```

3. Start:

```bash
docker compose up --build -d
```

4. Point Splunk forwarders at this host using [`splunk/outputs.conf`](splunk/outputs.conf):

- Cooked S2S → `:39998`
- Uncooked plain → `:39997`

Ingest ports bind to `127.0.0.1` by default (`INGEST_BIND`). For remote forwarders
set `INGEST_BIND=0.0.0.0` in `.env`.

At startup, classify ensures the ECS index template. `/health` stays 503 until
that succeeds.

Optional metrics stack (Prometheus scrape + remote_write + index-lag probe):

```bash
docker compose --profile metrics up -d --build
```

## Horizontal scaling (shards)

Each shard is a full compose project (own classify + writer) with **offset host
ports** so several stacks can share one machine. Planning floor: **~0.008 GB/s
per writer stack** (`python -m loadtest run -s S1`).

```bash
./scripts/run-shard.sh 0 up --build -d
./scripts/run-shard.sh 1 up --build -d
```

Production: Helm StatefulSet replicas = shard count, shared classify Deployment.
See [`docs/runbooks/sharding.md`](docs/runbooks/sharding.md).

## Ports

| Port | Service | Role |
|------|---------|------|
| 39998 | writer | Cooked Splunk S2S |
| 39997 | writer | Uncooked TCP |
| 8080 | classify | Health + ensure API |
| 8081 | writer | Health + `/metrics` |
| 9090 | prometheus | UI (metrics profile) |
| 9103 | index-lag-probe | Lag gauge (metrics profile) |

## Layout

```
├── sidecar/                  # classify FastAPI
├── packages/splastic-writer/ # cooked + uncooked → ES bulk
├── loadtest/                 # synthetic S2S / TCP generators
├── deploy/
│   ├── helm/splastic/        # Kubernetes chart
│   ├── alerts/               # Prometheus alert + recording rules
│   └── prometheus/           # scrape + remote_write
├── docs/                     # contracts + runbooks
├── scripts/                  # run-shard, compute baseline
├── splunk/                   # sample outputs.conf
├── testdata/s2s/             # golden S2S fixtures
└── docker-compose.yml
```

## Docs

| Doc | Topic |
|-----|-------|
| [`PERFORMANCE.md`](PERFORMANCE.md) | Architecture + measured floor |
| [`docs/contracts/s2s-ndjson.md`](docs/contracts/s2s-ndjson.md) | Writer event schema |
| [`docs/runbooks/sharding.md`](docs/runbooks/sharding.md) | Multi-writer / VIP |
| [`docs/runbooks/compute-optimize.md`](docs/runbooks/compute-optimize.md) | GB/s phases |
| [`docs/runbooks/alerting.md`](docs/runbooks/alerting.md) | Alerts + scrape |
| [`docs/runbooks/spill.md`](docs/runbooks/spill.md) | Bulk-fail spill / replay |
| [`loadtest/README.md`](loadtest/README.md) | Load harness |
| [`deploy/helm/splastic/README.md`](deploy/helm/splastic/README.md) | Helm install |
