"""Unit tests for loadtest helpers (no running Splastic stack required)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loadtest.payloads import fields_for, uncooked_line  # noqa: E402
from loadtest.rate import RateLimiter  # noqa: E402
from loadtest.report import summarize  # noqa: E402
from loadtest.stats import GenStats  # noqa: E402

# shard_targets lives at loadtest/ root (sibling of package)
import shard_targets  # noqa: E402


def test_hot_payload_size():
    f = fields_for(hot=True, seq=1, event_bytes=1536)
    assert f["sourcetype"] == "access_combined"
    assert len(f["_raw"].encode()) >= 1536


def test_cold_payload_empty_metadata():
    f = fields_for(hot=False, seq=2, event_bytes=1024)
    assert f["sourcetype"] == ""
    assert f["source"] == ""


def test_uncooked_line_is_single_json_line():
    line = uncooked_line(hot=True, seq=3, event_bytes=800)
    assert line.endswith(b"\n")
    assert line.count(b"\n") == 1


def test_shard_targets_stride():
    assert shard_targets.cooked_ports(1) == [39998]
    assert shard_targets.cooked_ports(2) == [39998, 40008]
    assert shard_targets.cooked_ports(4) == [39998, 40008, 40018, 40028]
    urls = shard_targets.health_urls(2)
    assert urls == [
        "http://127.0.0.1:8081/health",
        "http://127.0.0.1:8091/health",
    ]


def test_shard_targets_rejects_zero():
    with pytest.raises(ValueError):
        shard_targets.cooked_ports(0)


def test_rate_limiter_approx_rate():
    async def _run() -> float:
        lim = RateLimiter(100.0, burst=10.0)
        n = 50
        t0 = asyncio.get_event_loop().time()
        for _ in range(n):
            await lim.acquire()
        return n / (asyncio.get_event_loop().time() - t0)

    eps = asyncio.run(_run())
    assert 50 < eps < 150


def test_summarize_pass_without_es(tmp_path: Path):
    csv_path = tmp_path / "m.csv"
    csv_path.write_text(
        "t_mono,gen_eps,s2s_upstream_queue,classify_ok,es_count\n"
        "1,1000,10,True,\n"
        "2,1000,20,True,\n",
        encoding="utf-8",
    )
    summary = summarize(
        metrics_csv=csv_path,
        gen_snapshot={"sent_events": 2000, "sent_bytes": 3_000_000, "elapsed_s": 2.0, "errors": 0},
        scenario_id="S1",
        phase="steady",
    )
    assert summary["passed"] is True
    assert summary["avg_eps"] == 1000.0
