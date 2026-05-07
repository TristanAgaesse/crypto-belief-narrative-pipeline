from __future__ import annotations

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

USER_AGENT = "crypto-belief-narrative-pipeline/0.1"
DEFAULT_TIMEOUT = 20.0

# HTTP statuses that are worth retrying. Other 4xx/5xx are deterministic and
# should fail fast so the operator sees the real problem.
_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class HttpError(RuntimeError):
    """Raised when an HTTP request fails with a non-2xx response.

    Carries ``status_code`` so callers (and the retry predicate) can decide
    whether to retry. ``status_code`` may be ``None`` when the failure happened
    before a response was returned.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, HttpError):
        return exc.status_code in _RETRYABLE_STATUSES
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def get_json(
    url: str,
    params: dict | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict | list:
    """Fetch JSON from a URL with retries on transient failures.

    Retries on transport errors and retryable HTTP statuses (408, 425, 429,
    5xx). Deterministic 4xx (auth, permissions, not found, bad request) are
    raised immediately so the failure surfaces clearly.
    """

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    with httpx.Client(headers=headers, timeout=timeout) as client:
        response = client.get(url, params=params)
        if response.status_code >= 300:
            body_preview = response.text[:200] if response.text else ""
            raise HttpError(
                f"HTTP {response.status_code} for {url} params={params} body={body_preview!r}",
                status_code=response.status_code,
            )
        return response.json()
