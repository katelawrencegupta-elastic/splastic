# Shared S2S golden fixtures

Binary corpora and `manifest.json` exercise the production Python writer decode
path (`packages/splastic-writer`, pytest `tests/test_golden.py`).
`packages/s2s-decode` is a symlink to that package.

Cooked ingest is Python-only. Protocol / framing fixes must update fixtures
here and pass the golden suite.

Regenerate / verify (from the repo root):

```bash
cd packages/splastic-writer && PYTHONPATH=. pytest tests/test_golden.py
```

Prefer editing the Python generators in `packages/splastic-writer/s2s/testdata`
then extending golden helpers as needed.

Stats names (canonical): `frames_ok`, `frames_bad_magic`, `frames_bad_kv`,
`frames_oversized`. KV body parse failures are reported as errors prefixed
`kv:` so the decoder can classify them.

Normalized event fields after decode are documented in
[`docs/contracts/s2s-ndjson.md`](../../docs/contracts/s2s-ndjson.md).
