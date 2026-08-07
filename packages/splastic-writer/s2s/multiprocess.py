"""Multi-process writer supervisor (SO_REUSEPORT workers + aggregated :8081).

When ``WRITER_PROCESSES`` <= 1, runs a single ``s2s.server.main()`` (legacy).
When > 1, spawns N worker processes that share cooked/uncooked ports via
``reuse_port``, each with a private loopback health port. This process serves
aggregated ``/health`` and ``/metrics`` on ``S2S_HEALTH_PORT`` (default 8081).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from typing import Any

import httpx
from aiohttp import web

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("s2s.multiprocess")

WORKER_HEALTH_BASE = int(os.environ.get("WRITER_WORKER_HEALTH_BASE", "18081"))
HEALTH_PORT = int(os.environ.get("S2S_HEALTH_PORT", "8081"))
HEALTH_HOST = os.environ.get("S2S_HEALTH_HOST", "0.0.0.0")
SPLASH_SHARD_ID = os.environ.get("SPLASH_SHARD_ID", "").strip()
POD_NAME = os.environ.get("POD_NAME", "").strip()

# Stats keys summed across workers for aggregator /health and /metrics.
_SUM_KEYS = (
    "handshake_seen",
    "frames_ok",
    "frames_bad_magic",
    "frames_bad_kv",
    "frames_oversized",
    "events_emitted",
    "bytes_consumed",
    "capabilities_replied",
    "upstream_queue",
    "active_connections",
    "events_in",
    "indexed_ok",
    "indexed_fail",
    "bulk_requests",
    "ensure_calls",
    "classify_meta_hit",
    "classify_message_hit",
    "classify_generic",
)
# Take max (capacity knobs should match).
_MAX_KEYS = ("upstream_batch_size", "upstream_flush_ms", "max_connections")
_QUEUE_CAP_KEY = "upstream_queue_capacity"


def writer_processes() -> int:
    return max(1, int(os.environ.get("WRITER_PROCESSES", "1")))


def worker_health_port(index: int, base: int = WORKER_HEALTH_BASE) -> int:
    return base + index


def aggregate_stats(worker_bodies: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum / max worker ``stats`` dicts into one aggregate."""
    out: dict[str, Any] = {k: 0 for k in _SUM_KEYS}
    for key in _MAX_KEYS:
        out[key] = 0
    out[_QUEUE_CAP_KEY] = 0
    for body in worker_bodies:
        stats = body.get("stats") or {}
        for key in _SUM_KEYS:
            out[key] = int(out.get(key, 0)) + int(stats.get(key) or 0)
        for key in _MAX_KEYS:
            out[key] = max(int(out.get(key, 0)), int(stats.get(key) or 0))
        out[_QUEUE_CAP_KEY] = max(
            int(out.get(_QUEUE_CAP_KEY, 0)),
            int(stats.get(_QUEUE_CAP_KEY) or stats.get("upstream_queue_capacity") or 0),
        )
    # Capacity is per-worker; report fleet capacity as sum of queue caps.
    caps = []
    for body in worker_bodies:
        stats = body.get("stats") or {}
        caps.append(int(stats.get("upstream_queue_capacity") or 0))
    if caps:
        out[_QUEUE_CAP_KEY] = sum(caps) if any(caps) else out.get(_QUEUE_CAP_KEY, 0)
    return out


def _prom_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _agg_labels() -> str:
    if not SPLASH_SHARD_ID:
        return ""
    return f'{{shard="{_prom_escape(SPLASH_SHARD_ID)}"}}'


def metrics_text_from_stats(stats: dict[str, Any]) -> str:
    """Prometheus text for aggregated (no ``worker`` label) series."""
    lbl = _agg_labels()

    def m(name: str, value: float | int | str) -> str:
        return f"{name}{lbl} {value}"

    events = int(stats.get("events_emitted") or 0)
    bytes_c = int(stats.get("bytes_consumed") or 0)
    avg = (float(bytes_c) / float(events)) if events > 0 else 0.0
    lines = [
        "# HELP splash_s2s_handshake_seen_total Cooked-mode handshakes seen",
        "# TYPE splash_s2s_handshake_seen_total counter",
        m("splash_s2s_handshake_seen_total", stats.get("handshake_seen", 0)),
        "# HELP splash_s2s_frames_ok_total Successfully decoded S2S frames",
        "# TYPE splash_s2s_frames_ok_total counter",
        m("splash_s2s_frames_ok_total", stats.get("frames_ok", 0)),
        "# HELP splash_s2s_frames_bad_magic_total Frames rejected (bad magic/framing)",
        "# TYPE splash_s2s_frames_bad_magic_total counter",
        m("splash_s2s_frames_bad_magic_total", stats.get("frames_bad_magic", 0)),
        "# HELP splash_s2s_frames_bad_kv_total Frames rejected (KV/body parse)",
        "# TYPE splash_s2s_frames_bad_kv_total counter",
        m("splash_s2s_frames_bad_kv_total", stats.get("frames_bad_kv", 0)),
        "# HELP splash_s2s_frames_oversized_total Oversized frames rejected",
        "# TYPE splash_s2s_frames_oversized_total counter",
        m("splash_s2s_frames_oversized_total", stats.get("frames_oversized", 0)),
        "# HELP splash_s2s_events_emitted_total Events decoded from cooked S2S",
        "# TYPE splash_s2s_events_emitted_total counter",
        m("splash_s2s_events_emitted_total", stats.get("events_emitted", 0)),
        "# HELP splash_s2s_bytes_consumed_total Bytes read from Splunk clients",
        "# TYPE splash_s2s_bytes_consumed_total counter",
        m("splash_s2s_bytes_consumed_total", stats.get("bytes_consumed", 0)),
        "# HELP splash_s2s_avg_event_bytes Average bytes per emitted event (lifetime)",
        "# TYPE splash_s2s_avg_event_bytes gauge",
        m("splash_s2s_avg_event_bytes", f"{avg:.3f}"),
        "# HELP splash_s2s_upstream_queue Events waiting for Elasticsearch bulk",
        "# TYPE splash_s2s_upstream_queue gauge",
        m("splash_s2s_upstream_queue", stats.get("upstream_queue", 0)),
        "# HELP splash_s2s_upstream_queue_capacity Max bulk queue size",
        "# TYPE splash_s2s_upstream_queue_capacity gauge",
        m(
            "splash_s2s_upstream_queue_capacity",
            stats.get("upstream_queue_capacity", 0),
        ),
        "# HELP splash_s2s_active_connections Active S2S client connections",
        "# TYPE splash_s2s_active_connections gauge",
        m("splash_s2s_active_connections", stats.get("active_connections", 0)),
        "# HELP splash_s2s_max_connections Configured S2S connection cap",
        "# TYPE splash_s2s_max_connections gauge",
        m("splash_s2s_max_connections", stats.get("max_connections", 0)),
        "# HELP splash_writer_events_in_total Events accepted by ingest pipeline",
        "# TYPE splash_writer_events_in_total counter",
        m("splash_writer_events_in_total", stats.get("events_in", 0)),
        "# HELP splash_writer_indexed_ok_total Documents accepted by ES bulk",
        "# TYPE splash_writer_indexed_ok_total counter",
        m("splash_writer_indexed_ok_total", stats.get("indexed_ok", 0)),
        "# HELP splash_writer_indexed_fail_total Documents failed in ES bulk",
        "# TYPE splash_writer_indexed_fail_total counter",
        m("splash_writer_indexed_fail_total", stats.get("indexed_fail", 0)),
        "# HELP splash_writer_bulk_requests_total Elasticsearch _bulk HTTP calls",
        "# TYPE splash_writer_bulk_requests_total counter",
        m("splash_writer_bulk_requests_total", stats.get("bulk_requests", 0)),
        "# HELP splash_writer_ensure_calls_total POST /ensure/batch calls",
        "# TYPE splash_writer_ensure_calls_total counter",
        m("splash_writer_ensure_calls_total", stats.get("ensure_calls", 0)),
        "# HELP splash_writer_classify_meta_hit_total Metadata-path classify hits",
        "# TYPE splash_writer_classify_meta_hit_total counter",
        m("splash_writer_classify_meta_hit_total", stats.get("classify_meta_hit", 0)),
        "# HELP splash_writer_classify_message_hit_total Message-path classify hits",
        "# TYPE splash_writer_classify_message_hit_total counter",
        m(
            "splash_writer_classify_message_hit_total",
            stats.get("classify_message_hit", 0),
        ),
        "# HELP splash_writer_classify_generic_total Generic fallback classify",
        "# TYPE splash_writer_classify_generic_total counter",
        m("splash_writer_classify_generic_total", stats.get("classify_generic", 0)),
        "# HELP splash_writer_processes Writer OS processes in this pod",
        "# TYPE splash_writer_processes gauge",
        m("splash_writer_processes", writer_processes()),
        "",
    ]
    return "\n".join(lines)


def _spawn_workers(n: int) -> list[subprocess.Popen]:
    procs: list[subprocess.Popen] = []
    for i in range(n):
        env = os.environ.copy()
        env["WRITER_WORKER_INDEX"] = str(i)
        env["WRITER_REUSE_PORT"] = "1"
        env["S2S_HEALTH_HOST"] = "127.0.0.1"
        env["S2S_HEALTH_PORT"] = str(worker_health_port(i))
        # Avoid nested supervisors if children inherit WRITER_PROCESSES.
        env["WRITER_PROCESSES"] = "1"
        env["WRITER_ROLE"] = "worker"
        proc = subprocess.Popen(
            [sys.executable, "-m", "s2s.server"],
            env=env,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        procs.append(proc)
        logger.info(
            "spawned writer worker=%s pid=%s health=127.0.0.1:%s",
            i,
            proc.pid,
            worker_health_port(i),
        )
    return procs


async def _fetch_worker_health(
    client: httpx.AsyncClient, index: int
) -> dict[str, Any] | None:
    url = f"http://127.0.0.1:{worker_health_port(index)}/health"
    try:
        resp = await client.get(url, timeout=2.0)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


async def _fetch_worker_metrics(
    client: httpx.AsyncClient, index: int
) -> str:
    url = f"http://127.0.0.1:{worker_health_port(index)}/metrics"
    try:
        resp = await client.get(url, timeout=2.0)
        if resp.status_code != 200:
            return ""
        return resp.text
    except Exception:
        return ""


def _wait_workers_ready(n: int, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        ok = 0
        try:
            with httpx.Client(timeout=1.0) as client:
                for i in range(n):
                    r = client.get(f"http://127.0.0.1:{worker_health_port(i)}/health")
                    if r.status_code == 200 and (r.json() or {}).get("status") == "ok":
                        ok += 1
        except Exception:
            ok = 0
        if ok == n:
            logger.info("all %s writer workers healthy", n)
            return
        time.sleep(0.25)
    raise RuntimeError(f"writer workers not healthy in time ({ok}/{n})")


def run_supervisor(n: int) -> None:
    procs = _spawn_workers(n)

    def _terminate_all(signum: int, _frame: Any) -> None:
        logger.info("supervisor received signal %s; stopping workers", signum)
        for p in procs:
            if p.poll() is None:
                p.terminate()

    signal.signal(signal.SIGTERM, _terminate_all)
    signal.signal(signal.SIGINT, _terminate_all)

    try:
        _wait_workers_ready(n)
    except Exception:
        _terminate_all(signal.SIGTERM, None)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        raise

    async def health_handler(request: web.Request) -> web.Response:
        client: httpx.AsyncClient = request.app["http"]
        workers: list[dict[str, Any]] = []
        bodies: list[dict[str, Any]] = []
        for i in range(n):
            body = await _fetch_worker_health(client, i)
            entry: dict[str, Any] = {"worker": i, "ok": body is not None}
            if body:
                entry["status"] = body.get("status")
                entry["stats"] = body.get("stats")
                bodies.append(body)
            workers.append(entry)
        alive = [b for b in bodies if b.get("status") == "ok"]
        status = "ok" if alive else "degraded"
        agg = aggregate_stats(alive) if alive else aggregate_stats([])
        payload: dict[str, Any] = {
            "status": status,
            "writer_processes": n,
            "workers_ok": len(alive),
            "stats": agg,
            "workers": workers,
        }
        if SPLASH_SHARD_ID:
            payload["shard"] = SPLASH_SHARD_ID
        if POD_NAME:
            payload["pod"] = POD_NAME
        return web.json_response(payload)

    async def metrics_handler(request: web.Request) -> web.Response:
        client: httpx.AsyncClient = request.app["http"]
        bodies: list[dict[str, Any]] = []
        per_worker_metrics: list[str] = []
        for i in range(n):
            body = await _fetch_worker_health(client, i)
            if body and body.get("status") == "ok":
                bodies.append(body)
            text = await _fetch_worker_metrics(client, i)
            if text:
                per_worker_metrics.append(text.rstrip())
        agg = aggregate_stats(bodies) if bodies else aggregate_stats([])
        # Aggregated series (no worker label) for existing alerts + per-worker scrape.
        combined = metrics_text_from_stats(agg)
        if per_worker_metrics:
            combined = combined.rstrip() + "\n" + "\n".join(per_worker_metrics) + "\n"
        return web.Response(
            text=combined,
            content_type="text/plain; version=0.0.4",
            charset="utf-8",
        )

    async def on_start(app: web.Application) -> None:
        app["http"] = httpx.AsyncClient()

    async def on_cleanup(app: web.Application) -> None:
        await app["http"].aclose()
        logger.info("supervisor shutting down; terminating workers")
        for p in procs:
            if p.poll() is None:
                p.terminate()
        deadline = time.monotonic() + float(
            os.environ.get("S2S_UPSTREAM_DRAIN_TIMEOUT_S", "30")
        ) + 10
        for p in procs:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                logger.warning("worker pid=%s did not exit; killing", p.pid)
                p.kill()

    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    logger.info(
        "supervisor listening health=%s:%s workers=%s",
        HEALTH_HOST,
        HEALTH_PORT,
        n,
    )
    web.run_app(app, host=HEALTH_HOST, port=HEALTH_PORT, print=None)


def main() -> None:
    # Workers invoked as ``python -m s2s.server`` set WRITER_ROLE=worker and
    # WRITER_PROCESSES=1; never nest supervisors.
    if os.environ.get("WRITER_ROLE", "").strip() == "worker":
        from s2s.server import main as worker_main

        worker_main()
        return

    n = writer_processes()
    if n <= 1:
        from s2s.server import main as worker_main

        worker_main()
        return
    run_supervisor(n)


if __name__ == "__main__":
    main()
