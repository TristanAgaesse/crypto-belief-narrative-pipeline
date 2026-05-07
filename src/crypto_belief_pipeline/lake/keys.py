from __future__ import annotations

from crypto_belief_pipeline.config import get_settings


def full_s3_key(relative_key: str) -> str:
    """Apply configured `s3_prefix` to a lake-relative key."""

    s = get_settings()
    prefix = (s.s3_prefix or "").strip("/")
    if not prefix:
        return relative_key.lstrip("/")
    return f"{prefix}/{relative_key.lstrip('/')}"

