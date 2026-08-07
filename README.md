# Splash

Splunk → Elasticsearch ingest bridge. Terminates Splunk forwarder traffic (cooked S2S and uncooked TCP), classifies events into ECS data streams, ensures those streams exist, and indexes into Elastic Cloud as `logs-{dataset}-{namespace}`.

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

Shared rules live in [`sidecar/classify_rules.json`](sidecar/classify_rules.json) (synced to [`packages/splastic-writer/writer/classify_rules.json`](packages/splastic-writer/writer/classify_rules.json)).

- **Metadata hit** (`sourcetype` / `source` matches rules): classify in the writer. Call `POST /ensure/batch` only the first time a data stream is seen.
- **Metadata miss**: message-pattern classify in the writer (same rules as the former Logstash miss path).

Steady-state Splunk traffic with known sourcetype/source pays almost no ensure HTTP.

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

Ingest ports bind to `127.0.0.1` by default (`INGEST_BIND`). For remote forwarders set `INGEST_BIND=0.0.0.0` in `.env`.

At startup, classify ensures the ECS index template. `/health` stays 503 until that succeeds.

Failed ES bulk docs can spill to `WRITER_SPILL_DIR` (compose volume `writer_spill`).

## Horizontal scaling (shards)

Each shard is a full compose project (own classify + writer) with **offset host ports** so several stacks can share one machine. Re-baseline GB/s per shard after the Logstash removal (`python -m loadtest run -s S1`).

```bash
./scripts/run-shard.sh 0 up --build -d
./scripts/run-shard.sh 1 up --build -d
```

## Ports

| Port | Service | Role |
|------|---------|------|
| 39998 | writer | Cooked Splunk S2S |
| 39997 | writer | Uncooked TCP |
| 8080 | classify | Health + ensure API |
| 8081 | writer | Health + `/metrics` |

## Layout

```
├── sidecar/                # classify FastAPI
├── packages/
│   └── splastic-writer/    # cooked + uncooked → ES bulk
├── loadtest/
├── deploy/helm/splastic/
└── docker-compose.yml
```

## Docs

- [`PERFORMANCE.md`](PERFORMANCE.md) — data-flow and tuning
- [`docs/runbooks/sharding.md`](docs/runbooks/sharding.md)
- [`docs/runbooks/compute-optimize.md`](docs/runbooks/compute-optimize.md)
