"""Ensure data streams via splastic-classify /ensure/batch."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger("writer.ensure")


class StreamEnsurer:
    """First-seen stream cache + Bearer POST /ensure/batch (Ruby parity)."""

    def __init__(
        self,
        *,
        classify_url: str,
        auth_token: str,
        client: httpx.AsyncClient,
        namespace: str = "default",
    ) -> None:
        self._url = classify_url.rstrip("/") + "/ensure/batch"
        self._token = auth_token
        self._client = client
        self._namespace = namespace
        self._ensured: set[str] = set()
        self.ensure_calls = 0
        self.ensure_streams = 0
        self.ensure_failures = 0

    def already_ensured(self, stream: str) -> bool:
        return stream in self._ensured

    def mark(self, stream: str) -> None:
        if stream:
            self._ensured.add(stream)

    async def ensure(self, stream: str) -> str:
        """Return resolved stream name; marks cache even on soft failure."""
        if not stream:
            stream = f"logs-generic-{self._namespace}"
        if stream in self._ensured:
            return stream

        self.ensure_calls += 1
        self.ensure_streams += 1
        headers = {"content-type": "application/json"}
        if self._token:
            headers["authorization"] = f"Bearer {self._token}"
        try:
            resp = await self._client.post(
                self._url,
                json={"streams": [stream]},
                headers=headers,
            )
            if resp.status_code >= 400:
                logger.warning(
                    "ensure_batch HTTP %s: %s",
                    resp.status_code,
                    resp.text[:500],
                )
                self.ensure_failures += 1
                self._ensured.add(stream)
                return stream
            data: dict[str, Any] = resp.json()
            results = data.get("results") or []
            if results:
                item = results[0]
                resolved = str(item.get("resolved_stream") or stream)
                ok = item.get("ok") is True or str(item.get("ok")).lower() == "true"
                if not ok:
                    self.ensure_failures += 1
                self._ensured.add(resolved)
                self._ensured.add(stream)
                return resolved or stream
        except Exception as exc:
            logger.warning("ensure_batch failed: %s", exc)
            self.ensure_failures += 1

        self._ensured.add(stream)
        return stream
