"""Shared S3/botocore client-error classification for lake I/O."""

from __future__ import annotations

from botocore.exceptions import ClientError

_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


def is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in _NOT_FOUND_CODES
    return False


__all__ = ["is_not_found", "_NOT_FOUND_CODES"]
