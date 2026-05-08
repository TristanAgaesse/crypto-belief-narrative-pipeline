from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import yaml
from gdeltdoc.filters import Filters
from gdeltdoc.helpers import load_json

from crypto_belief_pipeline.collectors.http import USER_AGENT

logger = logging.getLogger(__name__)

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_JSON_PARSE_MAX_DEPTH = 100

_throttle_lock = threading.Lock()
_last_gdelt_request_monotonic: float = 0.0

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_TIMESTAMP_COLS = ("datetime", "timestamp", "date", "Date", "DateTime")
_VOLUME_COLS = ("Volume Intensity", "volume_intensity", "mention_volume", "volume", "Value")
_TONE_COLS = ("avg_tone", "tone", "AvgTone", "Tone")


def load_narratives_config(path: str | Path = "config/narratives.yaml") -> dict:
    """Load the narratives config YAML and return its parsed dict."""

    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise TypeError(f"Expected mapping at top of {p}, got {type(data).__name__}")
    return data


def _keyword_for_filters(query: str) -> str | list[str]:
    """Map narratives YAML ``query`` strings to a ``Filters`` keyword argument.

    Config lines are often ``\"a\" OR \"b\"``. ``Filters`` wraps a string in an
    extra pair of quotes, producing invalid tokens like ``\"\"a\" OR \"b\"\"``.
    Passing a **list** makes ``gdeltdoc`` emit ``(\"a\" OR \"b\")``.
    """

    text = query.strip()
    if not text:
        return text

    parts = [p.strip() for p in text.split(" OR ")]
    stripped: list[str] = []
    for p in parts:
        if len(p) >= 2 and p[0] == p[-1] and p[0] in "\"'":
            stripped.append(p[1:-1].strip())
        else:
            stripped.append(p)

    if len(stripped) > 1:
        return stripped
    return stripped[0]


def _format_ts(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).strftime(_TS_FORMAT)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in (_TS_FORMAT, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=UTC)
                return dt.strftime(_TS_FORMAT)
            except ValueError:
                continue
        return None
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN check without importing math
        return None
    return f


def _pick_first(row: dict, candidates: tuple[str, ...]) -> Any:
    for c in candidates:
        if c in row and row[c] is not None:
            return row[c]
    return None


def _df_rows(df: Any) -> list[dict]:
    """Convert a pandas-like DataFrame (or list of dicts) into a list of plain dicts."""

    if df is None:
        return []
    if isinstance(df, list):
        return [r for r in df if isinstance(r, dict)]
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        try:
            rows = to_dict(orient="records")
        except TypeError:
            rows = to_dict("records")
        return [r for r in rows if isinstance(r, dict)]
    return []


def _rows_to_records(rows: list[dict], narrative: str, query: str) -> list[dict]:
    def _jsonable(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (str, int, bool)):
            return v
        if isinstance(v, float):
            return None if v != v else v  # NaN -> None
        if isinstance(v, datetime):
            dt = v if v.tzinfo else v.replace(tzinfo=UTC)
            return dt.astimezone(UTC).strftime(_TS_FORMAT)
        # pandas Timestamp / numpy scalars often expose `.to_pydatetime()` / `.item()`
        to_pydatetime = getattr(v, "to_pydatetime", None)
        if callable(to_pydatetime):
            try:
                return _jsonable(to_pydatetime())
            except Exception:
                return str(v)
        item = getattr(v, "item", None)
        if callable(item):
            try:
                return _jsonable(item())
            except Exception:
                return str(v)
        if isinstance(v, dict):
            return {str(k): _jsonable(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_jsonable(x) for x in v]
        return str(v)

    out: list[dict] = []
    for row in rows:
        ts_raw = _pick_first(row, _TIMESTAMP_COLS)
        ts = _format_ts(ts_raw)
        if ts is None:
            continue
        mention_volume = _coerce_float(_pick_first(row, _VOLUME_COLS))
        if mention_volume is None:
            continue
        avg_tone = _coerce_float(_pick_first(row, _TONE_COLS))
        out.append(
            {
                "timestamp": ts,
                "narrative": narrative,
                "query": query,
                "mention_volume": mention_volume,
                "avg_tone": avg_tone,
                "source": "gdelt",
                "raw": _jsonable(row),
            }
        )
    return out


def _gdelt_throttle(min_interval_sec: float) -> None:
    """Enforce a minimum wall-clock gap between GDELT Doc API calls (global, thread-safe)."""

    global _last_gdelt_request_monotonic
    if min_interval_sec <= 0:
        return
    with _throttle_lock:
        now = time.monotonic()
        wait = min_interval_sec - (now - _last_gdelt_request_monotonic)
        if wait > 0:
            time.sleep(wait)
        _last_gdelt_request_monotonic = time.monotonic()


def _timeline_response_to_dataframe(timeline: dict, mode: str) -> pd.DataFrame:
    """Build a DataFrame from GDELT timeline JSON (same framing as ``gdeltdoc``)."""

    if timeline == {} or not timeline.get("timeline") or len(timeline["timeline"]) == 0:
        return pd.DataFrame()

    series_list: list[dict[str, Any]] = timeline["timeline"]
    results: dict[str, Any] = {
        "datetime": [entry["date"] for entry in series_list[0]["data"]],
    }
    for series in series_list:
        results[series["series"]] = [entry["value"] for entry in series["data"]]

    if mode == "timelinevolraw":
        results["All Articles"] = [entry["norm"] for entry in series_list[0]["data"]]

    formatted = pd.DataFrame(results)
    formatted["datetime"] = pd.to_datetime(formatted["datetime"])
    return formatted


def _gdelt_doc_request(
    mode: str,
    query_params: dict[str, Any],
    *,
    min_interval_sec: float,
    max_attempts: int,
    timeout: float,
) -> dict:
    """GET GDELT Doc JSON with spacing, retries on 429 / transient 5xx / transport errors."""

    # `gdeltdoc.Filters.query_string` includes URL query params (e.g. `&startdatetime=...`).
    # We must pass a proper params dict instead of embedding `&...` inside the `query` value,
    # otherwise GDELT returns an HTML error about illegal characters.
    params = {**query_params, "mode": mode, "format": "json"}
    headers = {"User-Agent": f"{USER_AGENT} gdelt-doc", "Accept": "application/json"}
    last_exc: BaseException | None = None

    for attempt in range(max_attempts):
        _gdelt_throttle(min_interval_sec)
        try:
            with httpx.Client(headers=headers, timeout=timeout) as client:
                resp = client.get(GDELT_DOC_URL, params=params)
        except httpx.TransportError as e:
            last_exc = e
            sleep_s = min(5.0 * (2**attempt), 90.0)
            logger.warning(
                "GDELT transport error (attempt %s/%s): %s",
                attempt + 1,
                max_attempts,
                e,
            )
            time.sleep(sleep_s)
            continue

        if resp.status_code == 200:
            content_type = resp.headers.get("content-type", "")
            if "text/html" in content_type:
                raise ValueError(f"GDELT returned HTML (invalid query?): {resp.text.strip()[:500]}")
            return load_json(resp.content, _JSON_PARSE_MAX_DEPTH)

        if resp.status_code == 429:
            last_exc = RuntimeError(f"HTTP 429 {resp.text[:200]!r}")
            ra = resp.headers.get("Retry-After")
            try:
                wait_s = float(ra) if ra is not None else 0.0
            except ValueError:
                wait_s = 0.0
            if wait_s <= 0.0:
                wait_s = 10.0 + 5.0 * attempt
            wait_s = max(wait_s, min_interval_sec)
            logger.warning(
                "GDELT rate limited (429), sleeping %.1fs (attempt %s/%s)",
                wait_s,
                attempt + 1,
                max_attempts,
            )
            time.sleep(wait_s)
            continue

        if resp.status_code in (502, 503, 504):
            last_exc = RuntimeError(f"HTTP {resp.status_code}")
            sleep_s = min(5.0 * (2**attempt), 90.0)
            logger.warning(
                "GDELT server error %s, sleeping %.1fs (attempt %s/%s)",
                resp.status_code,
                sleep_s,
                attempt + 1,
                max_attempts,
            )
            time.sleep(sleep_s)
            continue

        if 400 <= resp.status_code < 500:
            raise RuntimeError(
                f"GDELT client error {resp.status_code}: {resp.text[:400]!r}"
            ) from None

        last_exc = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]!r}")
        sleep_s = min(5.0 * (2**attempt), 90.0)
        time.sleep(sleep_s)

    msg = f"GDELT request failed after {max_attempts} attempts (mode={mode!r})"
    raise RuntimeError(msg) from last_exc


def _filters_query_params(filters: Filters) -> dict[str, Any]:
    """Convert gdeltdoc Filters.query_params into a requests-style params dict.

    gdeltdoc exposes `Filters.query_params` as a list:
      ['"bitcoin ETF" ', '&startdatetime=...', '&enddatetime=...', '&maxrecords=250']

    We must not embed the `&...` fragments into the `query` value.
    """

    qp = getattr(filters, "query_params", None)
    if isinstance(qp, dict):
        return qp
    if isinstance(qp, list) and qp:
        query = str(qp[0]).strip()
        params: dict[str, Any] = {"query": query}
        for frag in qp[1:]:
            text = str(frag).strip()
            if text.startswith("&"):
                text = text[1:]
            if "=" in text:
                k, v = text.split("=", 1)
                params[k] = v
        return params
    # Fallback: use query_string but strip everything after the first space.
    qs = str(getattr(filters, "query_string", "")).strip()
    query_only = qs.split(" ", 1)[0] if qs else ""
    return {"query": query_only}


def fetch_timelinevol(
    narrative: str,
    query: str,
    start_date: str,
    end_date: str,
    *,
    min_interval_sec: float = 5.5,
    max_attempts: int = 8,
    timeout: float = 60.0,
) -> list[dict]:
    """Fetch a TimelineVol series for one narrative and return Step 2-shaped records."""

    filters = Filters(
        keyword=_keyword_for_filters(query),
        start_date=start_date,
        end_date=end_date,
    )
    timeline = _gdelt_doc_request(
        "timelinevol",
        _filters_query_params(filters),
        min_interval_sec=min_interval_sec,
        max_attempts=max_attempts,
        timeout=timeout,
    )
    df = _timeline_response_to_dataframe(timeline, mode="timelinevol")
    rows = _df_rows(df)
    return _rows_to_records(rows, narrative=narrative, query=query)


def collect_gdelt_raw(
    start_date: str,
    end_date: str,
    narratives_config_path: str | Path = "config/narratives.yaml",
    *,
    min_interval_sec: float = 5.5,
    max_attempts: int = 8,
    timeout: float = 60.0,
    collection_stats: dict[str, Any] | None = None,
) -> list[dict]:
    """Iterate narratives from the config and concatenate their TimelineVol records."""

    cfg = load_narratives_config(narratives_config_path)
    narratives = cfg.get("narratives") or {}
    if not isinstance(narratives, dict):
        if collection_stats is not None:
            collection_stats.clear()
            collection_stats.update(
                {
                    "gdelt_narratives_attempted": 0,
                    "gdelt_narratives_failed": 0,
                    "gdelt_failure_details": [],
                }
            )
        return []

    out: list[dict] = []
    attempted = 0
    failures: list[dict[str, str]] = []
    for name, spec in narratives.items():
        if not isinstance(spec, dict):
            continue
        query = spec.get("query")
        if not isinstance(query, str) or not query.strip():
            continue
        attempted += 1
        try:
            rows = fetch_timelinevol(
                narrative=name,
                query=query,
                start_date=start_date,
                end_date=end_date,
                min_interval_sec=min_interval_sec,
                max_attempts=max_attempts,
                timeout=timeout,
            )
        except Exception as e:
            logger.warning("GDELT narrative %r failed: %s", name, e)
            failures.append({"narrative": str(name), "error": str(e)[:500]})
            rows = []
        out.extend(rows)
    if collection_stats is not None:
        collection_stats.clear()
        collection_stats.update(
            {
                "gdelt_narratives_attempted": attempted,
                "gdelt_narratives_failed": len(failures),
                "gdelt_failure_details": failures[:50],
            }
        )
    return out


def collect_gdelt_raw_window(
    *,
    start_time: datetime,
    end_time: datetime,
    narratives_config_path: str | Path = "config/narratives.yaml",
    min_interval_sec: float = 5.5,
    max_attempts: int = 8,
    timeout: float = 60.0,
) -> tuple[list[dict], dict[str, Any]]:
    """Windowed wrapper around `collect_gdelt_raw` using UTC dates.

    GDELT TimelineVol is effectively date-granular in this pipeline; we convert datetimes to dates.
    Returns (records, metadata).
    """

    st = start_time.astimezone(UTC)
    et = end_time.astimezone(UTC)
    stats: dict[str, Any] = {}
    rows = collect_gdelt_raw(
        start_date=st.date().isoformat(),
        end_date=et.date().isoformat(),
        narratives_config_path=narratives_config_path,
        min_interval_sec=min_interval_sec,
        max_attempts=max_attempts,
        timeout=timeout,
        collection_stats=stats,
    )
    ts = [r["timestamp"] for r in rows if isinstance(r.get("timestamp"), str)]
    meta: dict[str, Any] = {
        "source": "gdelt",
        "start_time": st.strftime(_TS_FORMAT),
        "end_time": et.strftime(_TS_FORMAT),
        "load_timestamp": datetime.now(UTC).replace(microsecond=0).strftime(_TS_FORMAT),
        "records": len(rows),
        "min_event_time": min(ts) if ts else None,
        "max_event_time": max(ts) if ts else None,
        **stats,
    }
    return rows, meta
