"""TCP terminator: cooked S2S + uncooked TCP → classify → Elasticsearch bulk."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx
from aiohttp import web

from s2s.decoder import (
    DEFAULT_MAX_SESSION_BUFFER_BYTES,
    S2SSession,
    S2SStats,
    SessionBufferExceeded,
)
from s2s.framing import DEFAULT_MAX_FRAME_SIZE
from writer.bulk import SHUTDOWN, BulkIndexer, BulkStats, bulk_worker
from writer.ensure import StreamEnsurer
from writer.pipeline import IngestPipeline
from writer.uncooked import handle_uncooked_client

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("s2s.server")

S2S_LISTEN_HOST = os.environ.get("S2S_LISTEN_HOST", "0.0.0.0")
S2S_LISTEN_PORT = int(os.environ.get("S2S_LISTEN_PORT", "39998"))
UNCOOKED_LISTEN_HOST = os.environ.get("UNCOOKED_LISTEN_HOST", "0.0.0.0")
UNCOOKED_LISTEN_PORT = int(os.environ.get("UNCOOKED_LISTEN_PORT", "39997"))
HEALTH_PORT = int(os.environ.get("S2S_HEALTH_PORT", "8081"))
MAX_FRAME_SIZE = int(os.environ.get("S2S_MAX_FRAME_SIZE", str(DEFAULT_MAX_FRAME_SIZE)))
MAX_CONNECTIONS = int(os.environ.get("S2S_MAX_CONNECTIONS", "256"))
MAX_SESSION_BUFFER_BYTES = int(
    os.environ.get("S2S_MAX_SESSION_BUFFER_BYTES", str(DEFAULT_MAX_SESSION_BUFFER_BYTES))
)
UPSTREAM_QUEUE_SIZE = int(os.environ.get("S2S_UPSTREAM_QUEUE_SIZE", "10000"))
UPSTREAM_BATCH_SIZE = int(os.environ.get("S2S_UPSTREAM_BATCH_SIZE", "1000"))
UPSTREAM_FLUSH_MS = int(os.environ.get("S2S_UPSTREAM_FLUSH_MS", "25"))
UPSTREAM_DRAIN_TIMEOUT_S = float(os.environ.get("S2S_UPSTREAM_DRAIN_TIMEOUT_S", "30"))
BULK_WORKERS = int(os.environ.get("WRITER_BULK_WORKERS", "16"))
# Multi-process: set by supervisor. Empty = single-process / no worker label.
WRITER_WORKER_INDEX = os.environ.get("WRITER_WORKER_INDEX", "").strip()
REUSE_PORT = os.environ.get("WRITER_REUSE_PORT", "0").lower() in (
    "1",
    "true",
    "yes",
)

ELASTIC_HOST = os.environ.get("ELASTIC_HOST", "").strip()
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY", "").strip()
CLASSIFY_URL = os.environ.get("CLASSIFY_URL", "http://classify:8080").rstrip("/")
CLASSIFY_AUTH_TOKEN = os.environ.get("CLASSIFY_AUTH_TOKEN", "")
DATA_STREAM_NAMESPACE = os.environ.get("DATA_STREAM_NAMESPACE", "default")
HTTP_TIMEOUT_S = float(os.environ.get("WRITER_HTTP_TIMEOUT_S", "30"))
# Optional fleet label for Prometheus (compose SHARD_ID or K8s ordinal/name).
SPLASTIC_SHARD_ID = os.environ.get("SPLASTIC_SHARD_ID", "").strip()
POD_NAME = os.environ.get("POD_NAME", "").strip()
HEALTH_HOST = os.environ.get("S2S_HEALTH_HOST", "0.0.0.0")


def _spill_dir() -> str | None:
    base = os.environ.get("WRITER_SPILL_DIR", "").strip() or None
    if not base:
        return None
    if WRITER_WORKER_INDEX != "":
        from pathlib import Path

        return str(Path(base) / f"worker-{WRITER_WORKER_INDEX}")
    return base


def _prom_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
    )


def _prom_labels() -> str:
    """Optional ``shard`` / ``worker`` labels for multi-shard and multi-process."""
    parts: list[str] = []
    if SPLASTIC_SHARD_ID:
        parts.append(f'shard="{_prom_escape(SPLASTIC_SHARD_ID)}"')
    if WRITER_WORKER_INDEX != "":
        parts.append(f'worker="{_prom_escape(WRITER_WORKER_INDEX)}"')
    if not parts:
        return ""
    return "{" + ",".join(parts) + "}"


def _m(name: str, value: float | int | str) -> str:
    return f"{name}{_prom_labels()} {value}"


async def handle_s2s_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    pipeline: IngestPipeline,
    stats: S2SStats,
) -> None:
    peer = writer.get_extra_info("peername")
    session = S2SSession(
        max_frame_size=MAX_FRAME_SIZE,
        max_session_buffer_bytes=MAX_SESSION_BUFFER_BYTES,
    )
    logger.info("S2S connection from %s", peer)
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            for event in session.feed(data):
                await pipeline.ingest(event)
            replies = session.take_replies()
            if replies:
                for reply in replies:
                    writer.write(reply)
                await writer.drain()
        for event in session.flush():
            await pipeline.ingest(event)
        replies = session.take_replies()
        if replies:
            for reply in replies:
                writer.write(reply)
            await writer.drain()
    except SessionBufferExceeded as exc:
        logger.warning("closing S2S connection from %s: %s", peer, exc)
    except Exception as exc:
        logger.exception("client handler error from %s: %s", peer, exc)
    finally:
        stats += session.stats
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        logger.info(
            "S2S connection closed %s frames_ok=%s events=%s caps_replied=%s",
            peer,
            session.stats.frames_ok,
            session.stats.events_emitted,
            session.stats.capabilities_replied,
        )


async def health(request: web.Request) -> web.Response:
    stats: S2SStats = request.app["stats"]
    queue: asyncio.Queue = request.app["bulk_queue"]
    bulk_stats: BulkStats = request.app["bulk_stats"]
    pipeline: IngestPipeline = request.app["pipeline"]
    ensurer: StreamEnsurer = request.app["ensurer"]
    body: dict[str, Any] = {
        "status": "ok",
        "stats": {
            "handshake_seen": stats.handshake_seen,
            "frames_ok": stats.frames_ok,
            "frames_bad_magic": stats.frames_bad_magic,
            "frames_bad_kv": stats.frames_bad_kv,
            "frames_oversized": stats.frames_oversized,
            "events_emitted": stats.events_emitted,
            "bytes_consumed": stats.bytes_consumed,
            "capabilities_replied": stats.capabilities_replied,
            "upstream_queue": queue.qsize(),
            "upstream_batch_size": UPSTREAM_BATCH_SIZE,
            "upstream_flush_ms": UPSTREAM_FLUSH_MS,
            "upstream_queue_capacity": UPSTREAM_QUEUE_SIZE,
            "active_connections": request.app["active_connections"],
            "max_connections": MAX_CONNECTIONS,
            "events_in": pipeline.events_in,
            "indexed_ok": bulk_stats.indexed_ok,
            "indexed_fail": bulk_stats.indexed_fail,
            "bulk_requests": bulk_stats.bulk_requests,
            "ensure_calls": ensurer.ensure_calls,
            "classify_meta_hit": pipeline.classify_meta_hit,
            "classify_message_hit": pipeline.classify_message_hit,
            "classify_generic": pipeline.classify_generic,
        },
    }
    if SPLASTIC_SHARD_ID:
        body["shard"] = SPLASTIC_SHARD_ID
    if POD_NAME:
        body["pod"] = POD_NAME
    if WRITER_WORKER_INDEX != "":
        body["worker"] = WRITER_WORKER_INDEX
    return web.json_response(body)


async def metrics(request: web.Request) -> web.Response:
    stats: S2SStats = request.app["stats"]
    queue: asyncio.Queue = request.app["bulk_queue"]
    bulk_stats: BulkStats = request.app["bulk_stats"]
    pipeline: IngestPipeline = request.app["pipeline"]
    ensurer: StreamEnsurer = request.app["ensurer"]
    avg_event_bytes = (
        float(stats.bytes_consumed) / float(stats.events_emitted)
        if stats.events_emitted > 0
        else 0.0
    )
    lines = [
        "# HELP splastic_s2s_handshake_seen_total Cooked-mode handshakes seen",
        "# TYPE splastic_s2s_handshake_seen_total counter",
        _m("splastic_s2s_handshake_seen_total", stats.handshake_seen),
        "# HELP splastic_s2s_frames_ok_total Successfully decoded S2S frames",
        "# TYPE splastic_s2s_frames_ok_total counter",
        _m("splastic_s2s_frames_ok_total", stats.frames_ok),
        "# HELP splastic_s2s_frames_bad_magic_total Frames rejected (bad magic/framing)",
        "# TYPE splastic_s2s_frames_bad_magic_total counter",
        _m("splastic_s2s_frames_bad_magic_total", stats.frames_bad_magic),
        "# HELP splastic_s2s_frames_bad_kv_total Frames rejected (KV/body parse)",
        "# TYPE splastic_s2s_frames_bad_kv_total counter",
        _m("splastic_s2s_frames_bad_kv_total", stats.frames_bad_kv),
        "# HELP splastic_s2s_frames_oversized_total Oversized frames rejected",
        "# TYPE splastic_s2s_frames_oversized_total counter",
        _m("splastic_s2s_frames_oversized_total", stats.frames_oversized),
        "# HELP splastic_s2s_events_emitted_total Events decoded from cooked S2S",
        "# TYPE splastic_s2s_events_emitted_total counter",
        _m("splastic_s2s_events_emitted_total", stats.events_emitted),
        "# HELP splastic_s2s_bytes_consumed_total Bytes read from Splunk clients",
        "# TYPE splastic_s2s_bytes_consumed_total counter",
        _m("splastic_s2s_bytes_consumed_total", stats.bytes_consumed),
        "# HELP splastic_s2s_avg_event_bytes Average bytes per emitted event (lifetime)",
        "# TYPE splastic_s2s_avg_event_bytes gauge",
        _m("splastic_s2s_avg_event_bytes", f"{avg_event_bytes:.3f}"),
        "# HELP splastic_s2s_upstream_queue Events waiting for Elasticsearch bulk",
        "# TYPE splastic_s2s_upstream_queue gauge",
        _m("splastic_s2s_upstream_queue", queue.qsize()),
        "# HELP splastic_s2s_upstream_queue_capacity Max bulk queue size",
        "# TYPE splastic_s2s_upstream_queue_capacity gauge",
        _m("splastic_s2s_upstream_queue_capacity", UPSTREAM_QUEUE_SIZE),
        "# HELP splastic_s2s_active_connections Active S2S client connections",
        "# TYPE splastic_s2s_active_connections gauge",
        _m("splastic_s2s_active_connections", request.app["active_connections"]),
        "# HELP splastic_s2s_max_connections Configured S2S connection cap",
        "# TYPE splastic_s2s_max_connections gauge",
        _m("splastic_s2s_max_connections", MAX_CONNECTIONS),
        "# HELP splastic_writer_events_in_total Events accepted by ingest pipeline",
        "# TYPE splastic_writer_events_in_total counter",
        _m("splastic_writer_events_in_total", pipeline.events_in),
        "# HELP splastic_writer_indexed_ok_total Documents accepted by ES bulk",
        "# TYPE splastic_writer_indexed_ok_total counter",
        _m("splastic_writer_indexed_ok_total", bulk_stats.indexed_ok),
        "# HELP splastic_writer_indexed_fail_total Documents failed in ES bulk",
        "# TYPE splastic_writer_indexed_fail_total counter",
        _m("splastic_writer_indexed_fail_total", bulk_stats.indexed_fail),
        "# HELP splastic_writer_bulk_requests_total Elasticsearch _bulk HTTP calls",
        "# TYPE splastic_writer_bulk_requests_total counter",
        _m("splastic_writer_bulk_requests_total", bulk_stats.bulk_requests),
        "# HELP splastic_writer_spill_writes_total Docs written to on-disk spill",
        "# TYPE splastic_writer_spill_writes_total counter",
        _m("splastic_writer_spill_writes_total", bulk_stats.spill_writes),
        "# HELP splastic_writer_ensure_calls_total POST /ensure/batch calls",
        "# TYPE splastic_writer_ensure_calls_total counter",
        _m("splastic_writer_ensure_calls_total", ensurer.ensure_calls),
        "# HELP splastic_writer_ensure_failures_total Ensure failures",
        "# TYPE splastic_writer_ensure_failures_total counter",
        _m("splastic_writer_ensure_failures_total", ensurer.ensure_failures),
        "# HELP splastic_writer_classify_meta_hit_total Metadata-path classify hits",
        "# TYPE splastic_writer_classify_meta_hit_total counter",
        _m("splastic_writer_classify_meta_hit_total", pipeline.classify_meta_hit),
        "# HELP splastic_writer_classify_message_hit_total Message-path classify hits",
        "# TYPE splastic_writer_classify_message_hit_total counter",
        _m("splastic_writer_classify_message_hit_total", pipeline.classify_message_hit),
        "# HELP splastic_writer_classify_generic_total Generic fallback classify",
        "# TYPE splastic_writer_classify_generic_total counter",
        _m("splastic_writer_classify_generic_total", pipeline.classify_generic),
        "",
    ]
    return web.Response(
        text="\n".join(lines),
        content_type="text/plain; version=0.0.4",
        charset="utf-8",
    )


def _spawn_bulk_workers(app: web.Application) -> list[asyncio.Task]:
    return [
        asyncio.create_task(
            bulk_worker(
                app["bulk_queue"],
                app["indexer"],
                batch_size=UPSTREAM_BATCH_SIZE,
                flush_ms=UPSTREAM_FLUSH_MS,
            ),
            name=f"bulk-worker-{i}",
        )
        for i in range(max(1, BULK_WORKERS))
    ]


async def _supervise_bulk(app: web.Application) -> None:
    while not app.get("shutting_down"):
        tasks: list[asyncio.Task] = app["bulk_tasks"]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if app.get("shutting_down"):
            return
        alive = [t for t in tasks if not t.done()]
        for task in tasks:
            if not task.done():
                continue
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                logger.warning("bulk worker exited unexpectedly; restarting: %s", exc)
            else:
                logger.warning("bulk worker exited unexpectedly; restarting")
            alive.append(
                asyncio.create_task(
                    bulk_worker(
                        app["bulk_queue"],
                        app["indexer"],
                        batch_size=UPSTREAM_BATCH_SIZE,
                        flush_ms=UPSTREAM_FLUSH_MS,
                    ),
                    name="bulk-worker-restart",
                )
            )
        app["bulk_tasks"] = alive


async def start_background(app: web.Application) -> None:
    if not ELASTIC_HOST or not ELASTIC_API_KEY:
        raise RuntimeError("ELASTIC_HOST and ELASTIC_API_KEY are required")

    queue: asyncio.Queue = asyncio.Queue(maxsize=UPSTREAM_QUEUE_SIZE)
    stats = S2SStats()
    bulk_stats = BulkStats()
    http = httpx.AsyncClient(timeout=HTTP_TIMEOUT_S)
    ensurer = StreamEnsurer(
        classify_url=CLASSIFY_URL,
        auth_token=CLASSIFY_AUTH_TOKEN,
        client=http,
        namespace=DATA_STREAM_NAMESPACE,
    )
    indexer = BulkIndexer(
        elastic_host=ELASTIC_HOST,
        api_key=ELASTIC_API_KEY,
        client=http,
        stats=bulk_stats,
        spill_dir=_spill_dir(),
    )
    pipeline = IngestPipeline(
        ensurer=ensurer,
        bulk_queue=queue,
        namespace=DATA_STREAM_NAMESPACE,
    )

    conn_sem = asyncio.Semaphore(MAX_CONNECTIONS)
    accept_lock = asyncio.Lock()
    app["bulk_queue"] = queue
    app["http_client"] = http
    app["ensurer"] = ensurer
    app["indexer"] = indexer
    app["pipeline"] = pipeline
    app["bulk_stats"] = bulk_stats
    app["bulk_tasks"] = _spawn_bulk_workers(app)
    app["supervisor_task"] = asyncio.create_task(
        _supervise_bulk(app), name="bulk-supervisor"
    )
    app["stats"] = stats
    app["active_connections"] = 0
    app["shutting_down"] = False
    app["conn_sem"] = conn_sem

    async def _s2s_cb(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        async with accept_lock:
            if conn_sem.locked():
                peer = writer.get_extra_info("peername")
                logger.warning(
                    "rejecting S2S connection from %s: at S2S_MAX_CONNECTIONS=%s",
                    peer,
                    MAX_CONNECTIONS,
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return
            await conn_sem.acquire()
            app["active_connections"] += 1
        try:
            await handle_s2s_client(reader, writer, pipeline, stats)
        finally:
            app["active_connections"] -= 1
            conn_sem.release()

    async def _uncooked_cb(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await handle_uncooked_client(reader, writer, pipeline.ingest)

    try:
        s2s_server = await asyncio.start_server(
            _s2s_cb,
            S2S_LISTEN_HOST,
            S2S_LISTEN_PORT,
            reuse_port=REUSE_PORT,
        )
        uncooked_server = await asyncio.start_server(
            _uncooked_cb,
            UNCOOKED_LISTEN_HOST,
            UNCOOKED_LISTEN_PORT,
            reuse_port=REUSE_PORT,
        )
    except OSError as exc:
        logger.error(
            "failed to bind ingest listeners (reuse_port=%s): %s", REUSE_PORT, exc
        )
        raise
    app["s2s_server"] = s2s_server
    app["uncooked_server"] = uncooked_server
    s2s_socks = ", ".join(str(s.getsockname()) for s in s2s_server.sockets or [])
    unc_socks = ", ".join(str(s.getsockname()) for s in uncooked_server.sockets or [])
    logger.info(
        "listening cooked=%s uncooked=%s (worker=%s reuse_port=%s bulk_workers=%s "
        "batch_size=%s flush_ms=%s queue=%s)",
        s2s_socks,
        unc_socks,
        WRITER_WORKER_INDEX or "solo",
        REUSE_PORT,
        BULK_WORKERS,
        UPSTREAM_BATCH_SIZE,
        UPSTREAM_FLUSH_MS,
        UPSTREAM_QUEUE_SIZE,
    )


async def stop_background(app: web.Application) -> None:
    app["shutting_down"] = True
    for key in ("s2s_server", "uncooked_server"):
        server: asyncio.AbstractServer = app[key]
        server.close()
        await server.wait_closed()
    logger.info("ingest listeners closed; draining bulk queue")

    queue: asyncio.Queue = app["bulk_queue"]
    tasks: list[asyncio.Task] = app["bulk_tasks"]
    supervisor: asyncio.Task | None = app.get("supervisor_task")
    # One shutdown sentinel per worker
    for _ in tasks:
        try:
            await asyncio.wait_for(queue.put(SHUTDOWN), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("could not enqueue shutdown sentinel; cancelling bulk workers")
            for t in tasks:
                t.cancel()
            break

    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=UPSTREAM_DRAIN_TIMEOUT_S,
        )
        logger.info("bulk workers drained and exited")
    except asyncio.TimeoutError:
        logger.warning(
            "bulk drain timed out after %ss with ~%s queued; cancelling",
            UPSTREAM_DRAIN_TIMEOUT_S,
            queue.qsize(),
        )
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if supervisor is not None and not supervisor.done():
        supervisor.cancel()
        try:
            await supervisor
        except asyncio.CancelledError:
            pass

    http: httpx.AsyncClient = app["http_client"]
    await http.aclose()


def main() -> None:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)
    app.on_startup.append(start_background)
    app.on_cleanup.append(stop_background)
    web.run_app(app, host=HEALTH_HOST, port=HEALTH_PORT, print=None)


if __name__ == "__main__":
    main()
