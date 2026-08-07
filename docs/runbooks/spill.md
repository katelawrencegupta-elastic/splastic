# Writer spill runbook — Splash

Failed Elasticsearch `_bulk` documents are written to an on-disk spill file so
they are not silently dropped during transient outages. There is no Logstash DLQ.

## Where it lives

| Deploy | Path |
|--------|------|
| Compose | volume `writer_spill` → `/var/lib/splastic/spill` (`WRITER_SPILL_DIR`) |
| Helm | per-pod PVC mounted at `/var/lib/splastic/spill` |

Spill file:

```text
/var/lib/splastic/spill/spill.ndjson
```

Each line pair is ES bulk NDJSON (`{ "create": { "_index": "…" } }` + document).
Default max size is 256 MiB; further failures are dropped with a warning once
the file is full.

Inspect:

```bash
# Compose
docker compose exec s2s-decode ls -la /var/lib/splastic/spill
docker compose exec s2s-decode du -sh /var/lib/splastic/spill

# Shard project
docker compose -p splash0 exec s2s-decode du -sh /var/lib/splastic/spill
```

Prometheus: `splash_writer_spill_writes_total`, `splash_writer_indexed_fail_total`.

## How to detect growth

1. Alert `SplashWriterBulkFailures` — `indexed_fail` increased over 15m (see
   [alerting.md](alerting.md)).
2. Rising `rate(splash_writer_spill_writes_total[5m])`.
3. Manual: `du -sh` / file size on the spill path.
4. Correlate with Elastic Cloud ingest pressure, mapping errors, and writer
   queue peg (`splash_s2s_upstream_queue`).

## When to replay vs drop

| Cause | Action |
|-------|--------|
| Transient ES outage / 429 / timeout | Replay after ES is healthy |
| Mapping / field conflict | Fix template/mappings; may need to drop poison docs |
| Malformed document | Drop or fix offline; do not infinite-replay |

## Replay procedure (outline)

Spill is raw `_bulk` NDJSON. Safer offline approach:

1. Stop or drain the affected writer (or copy the file while writers are paused
   for that shard).
2. Copy `spill.ndjson` out of the volume/PVC to a scratch directory.
3. Replay with `curl` (or equivalent) against ES `_bulk`:

   ```bash
   curl -sS -X POST "$ELASTIC_HOST/_bulk" \
     -H "Authorization: ApiKey $ELASTIC_API_KEY" \
     -H "Content-Type: application/x-ndjson" \
     --data-binary @spill.ndjson
   ```

4. Confirm indexed counts, then truncate or delete the spill file on the writer
   volume before restarting ingest on that shard.

Do not leave a growing spill unattended — once at max size, new failures are
dropped.

## After recovery

- Confirm `splash_writer_spill_writes_total` and `indexed_fail` are flat.
- Confirm `splash_s2s_upstream_queue` is not pegged.
- Note root cause in the incident log (ES capacity, mapping, credentials).
