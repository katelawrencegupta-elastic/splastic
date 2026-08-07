# Writer event schema (internal)

Events ingested by `packages/splastic-writer` (cooked S2S decode or uncooked TCP)
are normalized to a dict before classify + Elasticsearch `_bulk`.

## Fields

| Field | Notes |
|-------|-------|
| `message` | From `_raw` / `event.original` / plain line |
| `host`, `source`, `sourcetype`, `splunk_index` | Splunk metadata (`host::` stripped) |
| `@timestamp` | From `_time` (UNIX or ISO8601) when present |
| `tags` | Includes `s2s_decoded` / `splunk_tcp_39998` or `splunk_tcp_39997` |
| `data_stream.*`, `event.*`, `splunk.*` | Applied after classify |

## Routing

Bulk `create` action uses `_index` = `logs-{dataset}-{namespace}` (no ingest pipeline).

Classification runs **in-process** in the writer. The classify sidecar is used for
index-template readiness and first-seen `POST /ensure/batch` only — there is no
intermediate NDJSON hop to another pipeline.
