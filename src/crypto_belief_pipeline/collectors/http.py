from __future__ import annotations

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

USER_AGENT = "crypto-belief-narrative-pipeline/0.1"
DEFAULT_TIMEOUT = 20.0


class HttpError(RuntimeError):
    """Raised when an HTTP request fails with a non-2xx response."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.TransportError, HttpError)),
    reraise=True,
)
def get_json(
    url: str,
    params: dict | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict | list:
    """Fetch JSON from a URL with retries, timeout, and a project user-agent.

    Raises HttpError on non-2xx responses; raises httpx.TransportError on
    transport-level failures. Both are retried up to 3 times with
    exponential backoff.
    """

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    with httpx.Client(headers=headers, timeout=timeout) as client:
        response = client.get(url, params=params)
        if response.status_code >= 300:
            body_preview = response.text[:200] if response.text else ""
            raise HttpError(
                f"HTTP {response.status_code} for {url} params={params} body={body_preview!r}"
            )
        return response.json()
