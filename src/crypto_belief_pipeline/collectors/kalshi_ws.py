"""Optional Kalshi WebSocket feed (stub).

Authenticated streaming is gated on API credentials. When WS is not configured,
callers should skip connection and log once at module import or first use.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_WS_LOGGED_SKIP = False


def kalshi_ws_available() -> bool:
    """Return True when WS credentials are present (not implemented in MVP)."""

    return False


def connect_kalshi_ws_if_configured() -> None:
    """No-op unless WS support is added and env provides credentials."""

    global _WS_LOGGED_SKIP
    if kalshi_ws_available():
        return
    if not _WS_LOGGED_SKIP:
        logger.info("Kalshi WebSocket skipped: no credentials / not enabled in MVP.")
        _WS_LOGGED_SKIP = True


__all__ = ["connect_kalshi_ws_if_configured", "kalshi_ws_available"]
