"""Tests for multi-process supervisor helpers."""

from __future__ import annotations

from s2s.multiprocess import (
    aggregate_stats,
    metrics_text_from_stats,
    worker_health_port,
)


def test_worker_health_port_stride() -> None:
    assert worker_health_port(0, base=18081) == 18081
    assert worker_health_port(3, base=18081) == 18084


def test_aggregate_stats_sums_queues() -> None:
    bodies = [
        {
            "status": "ok",
            "stats": {
                "events_in": 10,
                "upstream_queue": 3,
                "upstream_queue_capacity": 10000,
                "indexed_ok": 10,
                "max_connections": 256,
            },
        },
        {
            "status": "ok",
            "stats": {
                "events_in": 7,
                "upstream_queue": 5,
                "upstream_queue_capacity": 10000,
                "indexed_ok": 7,
                "max_connections": 256,
            },
        },
    ]
    agg = aggregate_stats(bodies)
    assert agg["events_in"] == 17
    assert agg["upstream_queue"] == 8
    assert agg["upstream_queue_capacity"] == 20000
    assert agg["indexed_ok"] == 17
    assert agg["max_connections"] == 256


def test_metrics_text_includes_queue() -> None:
    text = metrics_text_from_stats(
        {"upstream_queue": 12, "events_emitted": 0, "bytes_consumed": 0}
    )
    assert "splastic_s2s_upstream_queue 12" in text
    assert "splastic_writer_processes" in text
