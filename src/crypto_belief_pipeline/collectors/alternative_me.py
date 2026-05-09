from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from crypto_belief_pipeline.collectors.http import get_json


@dataclass(frozen=True)
class FearGreedFetchResult:
    endpoint: str
    params: dict[str, Any]
    fetched_at: datetime
    ok: bool
    error: str | None
    raw_hash: str | None
    payload: dict[str, Any] | None


_DEFAULT_BASE_URL = "https://api.alternative.me"
_FNG_PATH = "/fng/"


def _stable_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash_payload(payload: Any) -> str | None:
    try:
        data = _stable_json_dumps(payload).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(data).hexdigest()


def _metadata_error(payload: dict[str, Any]) -> str | None:
    meta = payload.get("metadata")
    if not isinstance(meta, dict):
        return None
    err = meta.get("error")
    if err is None:
        return None
    text = str(err).strip()
    return text or "unknown_error"


def fetch_fear_and_greed(
    *,
    limit: int | None,
    base_url: str = _DEFAULT_BASE_URL,
) -> FearGreedFetchResult:
    """Fetch Alternative.me Fear & Greed index JSON.

    `limit=None` means "default/current" (no explicit limit param).
    `limit=0` means "full" per Alternative.me API behavior.
    """

    base = base_url.rstrip("/")
    endpoint = f"{base}{_FNG_PATH}"
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = int(limit)

    fetched_at = datetime.now(UTC).replace(microsecond=0)
    error: str | None = None
    payload_out: dict[str, Any] | None = None
    ok = False
    raw_hash: str | None = None

    try:
        payload = get_json(endpoint, params=params or None)
        if not isinstance(payload, dict):
            error = f"unexpected_payload_shape={type(payload).__name__}"
        else:
            payload_out = payload
            raw_hash = _hash_payload(payload)
            meta_err = _metadata_error(payload)
            if meta_err is not None:
                error = f"metadata.error={meta_err}"
            else:
                ok = True
    except Exception as e:
        error = str(e)[:500]

    return FearGreedFetchResult(
        endpoint=endpoint,
        params=params,
        fetched_at=fetched_at,
        ok=ok,
        error=error,
        raw_hash=raw_hash,
        payload=payload_out,
    )


__all__ = ["FearGreedFetchResult", "fetch_fear_and_greed"]
