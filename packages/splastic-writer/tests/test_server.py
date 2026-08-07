"""Tests for bulk queue batching and ES NDJSON shaping."""

from __future__ import annotations

import asyncio

import pytest

from writer.bulk import SHUTDOWN, BulkItem, BulkIndexer, BulkStats, _build_ndjson, _fill_batch, bulk_worker
from writer.normalize import normalize_event
from writer.uncooked import parse_uncooked_line
from writer.ecs import apply_classify
from writer.classify import classify_event
import s2s.server as server_mod


def test_prom_labels_empty_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_mod, "SPLASTIC_SHARD_ID", "")
    monkeypatch.setattr(server_mod, "WRITER_WORKER_INDEX", "")
    assert server_mod._prom_labels() == ""
    assert server_mod._m("splastic_s2s_upstream_queue", 3) == "splastic_s2s_upstream_queue 3"


def test_prom_labels_when_shard_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_mod, "SPLASTIC_SHARD_ID", "pipeline-0")
    monkeypatch.setattr(server_mod, "WRITER_WORKER_INDEX", "")
    assert server_mod._prom_labels() == '{shard="pipeline-0"}'
    assert (
        server_mod._m("splastic_s2s_upstream_queue", 3)
        == 'splastic_s2s_upstream_queue{shard="pipeline-0"} 3'
    )


def test_prom_labels_shard_and_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_mod, "SPLASTIC_SHARD_ID", "pipeline-0")
    monkeypatch.setattr(server_mod, "WRITER_WORKER_INDEX", "2")
    assert server_mod._prom_labels() == '{shard="pipeline-0",worker="2"}'


def test_spill_dir_appends_worker(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("WRITER_SPILL_DIR", str(tmp_path))
    monkeypatch.setattr(server_mod, "WRITER_WORKER_INDEX", "1")
    assert server_mod._spill_dir() == str(tmp_path / "worker-1")


def test_fill_batch_respects_size_and_flush():
    async def _run() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        for i in range(5):
            queue.put_nowait(
                BulkItem(target_stream="logs-x", pipeline="p", document={"i": i})
            )

        first = await queue.get()
        batch = await _fill_batch(queue, first, batch_size=3, flush_ms=0)
        assert isinstance(batch, list)
        assert len(batch) == 3
        assert queue.qsize() == 2

    asyncio.run(_run())


def test_fill_batch_propagates_shutdown_sentinel():
    async def _run() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        result = await _fill_batch(queue, SHUTDOWN, batch_size=10, flush_ms=50)
        assert result is SHUTDOWN

    asyncio.run(_run())


def test_build_ndjson_create_action():
    body = _build_ndjson(
        [
            BulkItem(
                target_stream="logs-access_log-loadtest",
                document={"message": "hi"},
            )
        ]
    )
    lines = body.decode().strip().split("\n")
    assert len(lines) == 2
    assert '"create"' in lines[0]
    assert "logs-access_log-loadtest" in lines[0]
    assert "pipeline" not in lines[0]
    assert '"message":"hi"' in lines[1].replace(" ", "")


def test_normalize_and_classify_hot_path():
    raw = {
        "sourcetype": "access_combined",
        "source": "/var/log/nginx/access.log",
        "host": "host::web1",
        "message": "GET /",
        "splunk_index": "",
        "_time": 1700000000,
        "tags": ["s2s_decoded"],
    }
    event = normalize_event(raw)
    assert event["host"] == "web1"
    assert event["@timestamp"].startswith("2023-")
    classified = classify_event(
        sourcetype=event["sourcetype"],
        source=event["source"],
        message=event["message"],
        splunk_index=event["splunk_index"],
    )
    doc, routing = apply_classify(event, classified, namespace="loadtest")
    assert routing.target_stream == "logs-access_log-loadtest"
    assert doc["data_stream"]["dataset"] == "access_log"
    assert "pipeline" not in doc.get("splunk", {})


def test_parse_uncooked_plain_and_json():
    plain = parse_uncooked_line("hello world")
    assert plain["message"] == "hello world"
    assert "splunk_tcp_39997" in plain["tags"]

    js = parse_uncooked_line(
        '{"message":"x","sourcetype":"access_combined","host":"h1"}'
    )
    assert js["sourcetype"] == "access_combined"
    assert "splunk_tcp_39997" in js["tags"]


def test_bulk_worker_sends_and_drains(monkeypatch):
    async def _run() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        sent: list[list[BulkItem]] = []

        class FakeIndexer(BulkIndexer):
            def __init__(self) -> None:
                self.stats = BulkStats()

            async def send(self, batch: list[BulkItem]) -> None:
                sent.append(list(batch))
                self.stats.indexed_ok += len(batch)

        indexer = FakeIndexer()
        await queue.put(
            BulkItem(target_stream="logs-generic-default", pipeline="p", document={"a": 1})
        )
        await queue.put(
            BulkItem(target_stream="logs-generic-default", pipeline="p", document={"a": 2})
        )
        task = asyncio.create_task(
            bulk_worker(queue, indexer, batch_size=2, flush_ms=0)
        )
        for _ in range(100):
            if sent:
                break
            await asyncio.sleep(0.01)
        await queue.put(SHUTDOWN)
        await asyncio.wait_for(task, timeout=2.0)
        assert len(sent) >= 1
        assert sum(len(b) for b in sent) == 2

    asyncio.run(_run())
