"""Normalize → classify → ensure → bulk enqueue."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from writer.bulk import BulkItem
from writer.classify import classify_event
from writer.ecs import apply_classify
from writer.ensure import StreamEnsurer
from writer.normalize import normalize_event

logger = logging.getLogger("writer.pipeline")


class IngestPipeline:
    def __init__(
        self,
        *,
        ensurer: StreamEnsurer,
        bulk_queue: asyncio.Queue,
        namespace: str = "default",
    ) -> None:
        self._ensurer = ensurer
        self._queue = bulk_queue
        self._namespace = namespace
        self.events_in = 0
        self.classify_meta_hit = 0
        self.classify_message_hit = 0
        self.classify_generic = 0

    async def ingest(self, raw: dict[str, Any]) -> None:
        self.events_in += 1
        event = normalize_event(raw)
        classified = classify_event(
            sourcetype=str(event.get("sourcetype") or ""),
            source=str(event.get("source") or ""),
            message=str(event.get("message") or ""),
            splunk_index=str(event.get("splunk_index") or ""),
        )
        if classified.reason.startswith("sourcetype=") or classified.reason.startswith(
            "source="
        ):
            self.classify_meta_hit += 1
        elif classified.reason.startswith("message="):
            self.classify_message_hit += 1
        else:
            self.classify_generic += 1

        doc, routing = apply_classify(
            event, classified, namespace=self._namespace, fallback=False
        )
        if self._ensurer.already_ensured(routing.target_stream):
            resolved = routing.target_stream
        else:
            resolved = await self._ensurer.ensure(routing.target_stream)
        item = BulkItem(
            target_stream=resolved,
            document=doc,
        )
        await self._queue.put(item)
