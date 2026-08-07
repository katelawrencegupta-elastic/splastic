"""Elasticsearch _bulk indexer (create action, no ingest pipeline)."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import orjson

logger = logging.getLogger("writer.bulk")

_SHUTDOWN = object()


@dataclass(frozen=True)
class BulkItem:
    target_stream: str
    document: dict[str, Any]
    # Kept optional for older call sites; ignored (no ES ingest pipeline).
    pipeline: str = ""


class BulkStats:
    def __init__(self) -> None:
        self.indexed_ok = 0
        self.indexed_fail = 0
        self.bulk_requests = 0
        self.bytes_sent = 0
        self.spill_writes = 0


async def _fill_batch(
    queue: asyncio.Queue,
    first: Any,
    *,
    batch_size: int,
    flush_ms: int,
) -> list[BulkItem] | object:
    if first is _SHUTDOWN:
        return _SHUTDOWN
    batch: list[BulkItem] = [first]
    if batch_size <= 1:
        return batch
    while len(batch) < batch_size:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if item is _SHUTDOWN:
            await queue.put(_SHUTDOWN)
            return batch
        batch.append(item)
    if len(batch) >= batch_size or flush_ms <= 0:
        return batch
    await asyncio.sleep(flush_ms / 1000.0)
    while len(batch) < batch_size:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if item is _SHUTDOWN:
            await queue.put(_SHUTDOWN)
            return batch
        batch.append(item)
    return batch


def _build_ndjson(batch: list[BulkItem]) -> bytes:
    parts: list[bytes] = []
    for item in batch:
        action = {"create": {"_index": item.target_stream}}
        parts.append(orjson.dumps(action))
        parts.append(orjson.dumps(item.document))
    return b"\n".join(parts) + b"\n"


def _auth_header(api_key: str) -> str:
    """Build Authorization for Elastic Cloud API keys.

    Accepts ``id:key`` (base64-encoded), already-base64 material, or a full
    ``ApiKey …`` header value. Raw ``ApiKey id:key`` is rejected (``:`` is
    not base64); pass ``id:key`` and this helper encodes it.
    """
    import base64

    raw = api_key.strip()
    if raw.lower().startswith("apikey "):
        return raw
    if ":" in raw:
        token = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        return f"ApiKey {token}"
    return f"ApiKey {raw}"


class BulkIndexer:
    def __init__(
        self,
        *,
        elastic_host: str,
        api_key: str,
        client: httpx.AsyncClient,
        stats: BulkStats | None = None,
        spill_dir: str | None = None,
        spill_max_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        self._url = elastic_host.rstrip("/") + "/_bulk"
        self._api_key = api_key
        self._client = client
        self.stats = stats or BulkStats()
        self._spill_dir = Path(spill_dir) if spill_dir else None
        self._spill_max = spill_max_bytes
        if self._spill_dir:
            self._spill_dir.mkdir(parents=True, exist_ok=True)

    async def send(self, batch: list[BulkItem]) -> None:
        if not batch:
            return
        body = _build_ndjson(batch)
        headers = {
            "content-type": "application/x-ndjson",
            "authorization": _auth_header(self._api_key),
        }
        self.stats.bulk_requests += 1
        self.stats.bytes_sent += len(body)
        try:
            resp = await self._client.post(self._url, content=body, headers=headers)
        except Exception as exc:
            logger.warning("bulk request failed: %s", exc)
            self.stats.indexed_fail += len(batch)
            await self._spill(batch)
            raise

        if resp.status_code >= 400:
            logger.warning("bulk HTTP %s: %s", resp.status_code, resp.text[:500])
            self.stats.indexed_fail += len(batch)
            await self._spill(batch)
            # Do not raise on 4xx auth/client errors — avoids infinite retry loops.
            if resp.status_code in (401, 403):
                return
            if resp.status_code >= 500:
                resp.raise_for_status()
            return

        payload = resp.json()
        if not payload.get("errors"):
            self.stats.indexed_ok += len(batch)
            return

        # Partial failures
        items = payload.get("items") or []
        failed: list[BulkItem] = []
        for i, entry in enumerate(items):
            create = entry.get("create") or {}
            status = int(create.get("status") or 0)
            if status >= 300:
                self.stats.indexed_fail += 1
                if i < len(batch):
                    failed.append(batch[i])
                err = create.get("error") or {}
                logger.warning(
                    "bulk item fail status=%s type=%s reason=%s",
                    status,
                    err.get("type"),
                    str(err.get("reason") or "")[:300],
                )
            else:
                self.stats.indexed_ok += 1
        if failed:
            await self._spill(failed)

    async def _spill(self, batch: list[BulkItem]) -> None:
        if not self._spill_dir or not batch:
            return
        path = self._spill_dir / "spill.ndjson"
        try:
            size = path.stat().st_size if path.exists() else 0
            if size >= self._spill_max:
                logger.warning("spill file at max size; dropping %s docs", len(batch))
                return
            with path.open("ab") as fh:
                fh.write(_build_ndjson(batch))
            self.stats.spill_writes += len(batch)
        except Exception as exc:
            logger.warning("spill write failed: %s", exc)


async def bulk_worker(
    queue: asyncio.Queue,
    indexer: BulkIndexer,
    *,
    batch_size: int = 100,
    flush_ms: int = 50,
) -> None:
    """Drain queue into ES bulk requests; retry inflight on transient errors."""
    inflight: list[BulkItem] = []
    batch_size = max(1, batch_size)
    while True:
        try:
            if not inflight:
                first = await queue.get()
                if first is _SHUTDOWN:
                    return
                filled = await _fill_batch(
                    queue, first, batch_size=batch_size, flush_ms=flush_ms
                )
                if filled is _SHUTDOWN:
                    return
                inflight = filled  # type: ignore[assignment]
            await indexer.send(inflight)
            inflight = []
        except asyncio.CancelledError:
            if inflight:
                try:
                    await indexer.send(inflight)
                    inflight = []
                except Exception as exc:
                    logger.warning(
                        "final bulk of %s docs failed: %s", len(inflight), exc
                    )
            raise
        except Exception as exc:
            logger.warning(
                "bulk worker error: %s; retrying %s docs after 1s",
                exc,
                len(inflight),
            )
            await asyncio.sleep(1.0)


SHUTDOWN = _SHUTDOWN
