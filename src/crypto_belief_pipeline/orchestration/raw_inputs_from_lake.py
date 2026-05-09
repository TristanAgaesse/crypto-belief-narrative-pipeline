"""Resolve raw JSONL lake keys for a daily partition without Dagster IO-manager inputs.

Bronze assets depend on ``raw_*`` for lineage, but when jobs omit the raw assets
(e.g. ad-hoc bronze-only materializations), Dagster cannot load upstream outputs from
ephemeral run storage. These helpers list objects under the lake prefix and pick
the same keys a co-located raw collector would emit (microbatch, CLI ``live_*``,
or ``sample_*`` layout).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from crypto_belief_pipeline.config import get_runtime_config, get_settings
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import list_jsonl_keys_under
from crypto_belief_pipeline.state.processing_watermarks import read_processing_watermark


def _candidate_buckets() -> list[str]:
    s = get_settings()
    out: list[str] = [s.s3_bucket]
    rt = get_runtime_config()
    sb = rt.sample.sample_lake_bucket if rt.sample.sample_enabled else None
    if sb and sb not in out:
        out.append(sb)
    return out


def _batch_id_from_micro_key(key: str) -> str:
    if "batch_id=" not in key:
        return ""
    segment = key.split("batch_id=", 1)[1]
    for suf in (
        "_markets.jsonl",
        "_prices.jsonl",
        "_events.jsonl",
        "_series.jsonl",
        "_trades.jsonl",
        "_orderbooks.jsonl",
        "_candlesticks.jsonl",
        ".jsonl",
    ):
        if segment.endswith(suf):
            return segment[: -len(suf)]
    return segment


def _batch_ts_from_key(key: str) -> datetime | None:
    bid = _batch_id_from_micro_key(key)
    return _batch_ts_from_batch_id(bid)


def _batch_ts_from_batch_id(batch_id: str) -> datetime | None:
    if not batch_id:
        return None
    core = batch_id.split("_", 1)[0]
    try:
        return datetime.strptime(core, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _dedupe_candidates(
    candidate_inputs: list[dict[str, str]],
    key_fields: tuple[str, ...],
) -> list[dict[str, str]]:
    seen: set[tuple[str, ...]] = set()
    out: list[dict[str, str]] = []
    for item in candidate_inputs:
        sig = tuple(str(item.get(k, "")) for k in key_fields)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(item)
    return out


def _is_batch_newer_than_watermark(
    candidate_batch_id: str,
    *,
    watermark_batch_id: str,
    watermark_ts: datetime,
) -> bool:
    candidate_ts = _batch_ts_from_batch_id(candidate_batch_id)
    if candidate_ts is None:
        # Keep malformed/legacy candidate ids for safety; downstream dedupe keeps
        # retries deterministic.
        return True
    if candidate_ts != watermark_ts:
        return candidate_ts > watermark_ts
    # Tie-break same-second microbatches by full id (suffix carries order).
    return candidate_batch_id > watermark_batch_id


def _is_microbatch_non_polymarket_jsonl(k: str) -> bool:
    return "/batch_id=" in k and k.endswith(".jsonl") and "_markets" not in k and "_prices" not in k


def _partition_dates_for_window(partition_start: datetime, partition_end: datetime) -> list[date]:
    if partition_end <= partition_start:
        return [partition_start.date()]
    out: list[date] = []
    cur = partition_start.date()
    end_date = (partition_end - timedelta(seconds=1)).date()
    while cur <= end_date:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _hourly_partition_key(partition_start: datetime) -> str:
    """Match Dagster :class:`HourlyPartitionsDefinition` keys: ``YYYY-MM-DD-HH:00``."""
    dt = partition_start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H:00")


def _safe_partition_slug(partition_key: str) -> str:
    return partition_key.replace(":", "-")


def _canonical_raw_hourly_key(*, dataset: str, partition_start: datetime) -> str:
    run_date = partition_start.astimezone(UTC).date()
    partition_key = _hourly_partition_key(partition_start)
    return (
        f"{partition_path('raw', dataset, run_date)}"
        f"/partition={_safe_partition_slug(partition_key)}/data.jsonl"
    )


def _list_window_jsonl_keys(
    dataset: str,
    partition_start: datetime,
    partition_end: datetime,
    *,
    bucket: str,
) -> set[str]:
    keys: set[str] = set()
    for run_date in _partition_dates_for_window(partition_start, partition_end):
        prefix = partition_path("raw", dataset, run_date)
        keys.update(list_jsonl_keys_under(prefix, bucket=bucket))
    return keys


def _polymarket_cli_or_sample(prefix: str, keys: set[str]) -> dict[str, str] | None:
    for mk_name, pk_name in (
        ("live_markets.jsonl", "live_prices.jsonl"),
        ("sample_markets.jsonl", "sample_prices.jsonl"),
    ):
        mk = f"{prefix}/{mk_name}"
        pk = f"{prefix}/{pk_name}"
        if mk in keys and pk in keys:
            return {"raw_polymarket_markets": mk, "raw_polymarket_prices": pk}
    return None


def resolve_raw_polymarket_for_partition(run_date: date) -> tuple[dict[str, str], str]:
    prefix = partition_path("raw", "provider=polymarket", run_date)
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys_list = list_jsonl_keys_under(prefix, bucket=bucket)
            keys = set(keys_list)
            micro_markets = sorted(
                k for k in keys if k.endswith("_markets.jsonl") and "/batch_id=" in k
            )
            if micro_markets:
                mk = micro_markets[-1]
                pk = mk.replace("_markets.jsonl", "_prices.jsonl")
                if pk not in keys:
                    raise FileNotFoundError(f"Polymarket prices missing for markets key {mk!r}")
                return (
                    {
                        "raw_polymarket_markets": mk,
                        "raw_polymarket_prices": pk,
                        "source_batch_id": _batch_id_from_micro_key(mk),
                        "source_window_start": "",
                        "source_window_end": "",
                    },
                    bucket,
                )
            pair = _polymarket_cli_or_sample(prefix, keys)
            if pair is None:
                raise FileNotFoundError(
                    f"No polymarket raw JSONL under {prefix!r} in bucket {bucket!r}; "
                    f"keys_sample={sorted(keys)[:30]}"
                )
            pair["source_batch_id"] = ""
            pair["source_window_start"] = ""
            pair["source_window_end"] = ""
            return pair, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(f"No polymarket raw under {prefix!r}")


def _no_raw_window_msg(*, dataset: str, partition_start: datetime, partition_end: datetime) -> str:
    return (
        f"No raw JSONL for dataset={dataset!r} "
        f"window=[{partition_start.isoformat()},{partition_end.isoformat()})"
    )


def list_raw_polymarket_for_partition_window(
    *,
    partition_start: datetime,
    partition_end: datetime,
    include_canonical_hourly: bool = False,
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            if include_canonical_hourly:
                mk = _canonical_raw_hourly_key(
                    dataset="provider=polymarket_markets",
                    partition_start=partition_start,
                )
                pk = _canonical_raw_hourly_key(
                    dataset="provider=polymarket_prices",
                    partition_start=partition_start,
                )
                mk_prefix = partition_path(
                    "raw", "provider=polymarket_markets", partition_start.astimezone(UTC).date()
                )
                pk_prefix = partition_path(
                    "raw", "provider=polymarket_prices", partition_start.astimezone(UTC).date()
                )
                mk_keys = set(list_jsonl_keys_under(mk_prefix, bucket=bucket))
                pk_keys = set(list_jsonl_keys_under(pk_prefix, bucket=bucket))
                if mk in mk_keys and pk in pk_keys:
                    canonical_batch_id = (
                        f"{partition_start.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_canonical"
                    )
                    return (
                        [
                            {
                                "raw_polymarket_markets": mk,
                                "raw_polymarket_prices": pk,
                                "source_batch_id": canonical_batch_id,
                                "source_window_start": "",
                                "source_window_end": "",
                            }
                        ],
                        bucket,
                    )
            keys = _list_window_jsonl_keys(
                "provider=polymarket", partition_start, partition_end, bucket=bucket
            )
            micro_markets = sorted(
                k for k in keys if k.endswith("_markets.jsonl") and "/batch_id=" in k
            )
            out: list[dict[str, str]] = []
            for mk in micro_markets:
                ts = _batch_ts_from_key(mk)
                if ts is None or not (partition_start <= ts < partition_end):
                    continue
                pk = mk.replace("_markets.jsonl", "_prices.jsonl")
                if pk not in keys:
                    continue
                out.append(
                    {
                        "raw_polymarket_markets": mk,
                        "raw_polymarket_prices": pk,
                        "source_batch_id": _batch_id_from_micro_key(mk),
                        "source_window_start": "",
                        "source_window_end": "",
                    }
                )
            return out, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(
        _no_raw_window_msg(
            dataset="provider=polymarket",
            partition_start=partition_start,
            partition_end=partition_end,
        )
    )


def resolve_raw_binance_for_partition(run_date: date) -> tuple[dict[str, str], str]:
    prefix = partition_path("raw", "provider=binance", run_date)
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = set(list_jsonl_keys_under(prefix, bucket=bucket))
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            if micro:
                chosen = micro[-1]
                bid = _batch_id_from_micro_key(chosen)
                return (
                    {
                        "raw_binance_klines": chosen,
                        "source_batch_id": bid,
                        "source_window_start": "",
                        "source_window_end": "",
                    },
                    bucket,
                )
            for name in ("live_klines.jsonl", "sample_klines.jsonl"):
                ck = f"{prefix}/{name}"
                if ck in keys:
                    return (
                        {
                            "raw_binance_klines": ck,
                            "source_batch_id": "",
                            "source_window_start": "",
                            "source_window_end": "",
                        },
                        bucket,
                    )
            raise FileNotFoundError(f"No binance raw JSONL under {prefix!r} in bucket {bucket!r}")
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(f"No binance raw under {prefix!r}")


def list_raw_binance_for_partition_window(
    *,
    partition_start: datetime,
    partition_end: datetime,
    include_canonical_hourly: bool = False,
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys(
                "provider=binance", partition_start, partition_end, bucket=bucket
            )
            if include_canonical_hourly:
                canonical_key = _canonical_raw_hourly_key(
                    dataset="provider=binance",
                    partition_start=partition_start,
                )
                if canonical_key in keys:
                    canonical_batch_id = (
                        f"{partition_start.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_canonical"
                    )
                    return (
                        [
                            {
                                "raw_binance_klines": canonical_key,
                                "source_batch_id": canonical_batch_id,
                                "source_window_start": "",
                                "source_window_end": "",
                            }
                        ],
                        bucket,
                    )
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            out: list[dict[str, str]] = []
            for chosen in micro:
                ts = _batch_ts_from_key(chosen)
                if ts is None or not (partition_start <= ts < partition_end):
                    continue
                out.append(
                    {
                        "raw_binance_klines": chosen,
                        "source_batch_id": _batch_id_from_micro_key(chosen),
                        "source_window_start": "",
                        "source_window_end": "",
                    }
                )
            return out, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(
        _no_raw_window_msg(
            dataset="provider=binance",
            partition_start=partition_start,
            partition_end=partition_end,
        )
    )


def resolve_raw_gdelt_for_partition(run_date: date) -> tuple[dict[str, str], str]:
    prefix = partition_path("raw", "provider=gdelt", run_date)
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = set(list_jsonl_keys_under(prefix, bucket=bucket))
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            if micro:
                chosen = micro[-1]
                bid = _batch_id_from_micro_key(chosen)
                return (
                    {
                        "raw_gdelt_timeline": chosen,
                        "source_batch_id": bid,
                        "source_window_start": "",
                        "source_window_end": "",
                    },
                    bucket,
                )
            for name in ("live_timeline.jsonl", "sample_timeline.jsonl"):
                ck = f"{prefix}/{name}"
                if ck in keys:
                    return (
                        {
                            "raw_gdelt_timeline": ck,
                            "source_batch_id": "",
                            "source_window_start": "",
                            "source_window_end": "",
                        },
                        bucket,
                    )
            # Optional source: allow empty timeline (GDELT often sparse).
            return (
                {
                    "raw_gdelt_timeline": f"{prefix}/live_timeline.jsonl",
                    "source_batch_id": "",
                    "source_window_start": "",
                    "source_window_end": "",
                },
                bucket,
            )
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(f"No gdelt prefix accessible {prefix!r}")


def list_raw_gdelt_for_partition_window(
    *,
    partition_start: datetime,
    partition_end: datetime,
    include_canonical_hourly: bool = False,
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys(
                "provider=gdelt", partition_start, partition_end, bucket=bucket
            )
            if include_canonical_hourly:
                canonical_key = _canonical_raw_hourly_key(
                    dataset="provider=gdelt",
                    partition_start=partition_start,
                )
                if canonical_key in keys:
                    canonical_batch_id = (
                        f"{partition_start.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_canonical"
                    )
                    return (
                        [
                            {
                                "raw_gdelt_timeline": canonical_key,
                                "source_batch_id": canonical_batch_id,
                                "source_window_start": "",
                                "source_window_end": "",
                            }
                        ],
                        bucket,
                    )
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            out: list[dict[str, str]] = []
            for chosen in micro:
                ts = _batch_ts_from_key(chosen)
                if ts is None or not (partition_start <= ts < partition_end):
                    continue
                out.append(
                    {
                        "raw_gdelt_timeline": chosen,
                        "source_batch_id": _batch_id_from_micro_key(chosen),
                        "source_window_start": "",
                        "source_window_end": "",
                    }
                )
            return out, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(
        _no_raw_window_msg(
            dataset="provider=gdelt",
            partition_start=partition_start,
            partition_end=partition_end,
        )
    )


def _kalshi_micro_keys_from_markets_key(markets_key: str) -> dict[str, str]:
    if not markets_key.endswith("_markets.jsonl"):
        raise ValueError(f"expected Kalshi markets micro key, got {markets_key!r}")
    base = markets_key[: -len("_markets.jsonl")]
    return {
        "raw_kalshi_markets": markets_key,
        "raw_kalshi_events": f"{base}_events.jsonl",
        "raw_kalshi_series": f"{base}_series.jsonl",
        "raw_kalshi_trades": f"{base}_trades.jsonl",
        "raw_kalshi_orderbooks": f"{base}_orderbooks.jsonl",
        "raw_kalshi_candlesticks": f"{base}_candlesticks.jsonl",
    }


def resolve_raw_fear_greed_for_partition(run_date: date) -> tuple[dict[str, str], str]:
    prefix = partition_path("raw", "provider=alternative_me_fear_greed", run_date)
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = set(list_jsonl_keys_under(prefix, bucket=bucket))
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            if micro:
                chosen = micro[-1]
                bid = _batch_id_from_micro_key(chosen)
                return (
                    {
                        "raw_fear_greed_payload": chosen,
                        "source_batch_id": bid,
                        "source_window_start": "",
                        "source_window_end": "",
                    },
                    bucket,
                )
            raise FileNotFoundError(
                f"No fear_greed raw JSONL under {prefix!r} in bucket {bucket!r}"
            )
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(f"No fear_greed raw under {prefix!r}")


def list_raw_kalshi_for_partition_window(
    *,
    partition_start: datetime,
    partition_end: datetime,
    include_canonical_hourly: bool = False,
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys(
                "provider=kalshi", partition_start, partition_end, bucket=bucket
            )
            if include_canonical_hourly:
                run_date = partition_start.astimezone(UTC).date()
                canonical_row: dict[str, str] = {}
                ok = True
                for dataset, field in (
                    ("provider=kalshi_markets", "raw_kalshi_markets"),
                    ("provider=kalshi_events", "raw_kalshi_events"),
                    ("provider=kalshi_series", "raw_kalshi_series"),
                    ("provider=kalshi_trades", "raw_kalshi_trades"),
                    ("provider=kalshi_orderbooks", "raw_kalshi_orderbooks"),
                    ("provider=kalshi_candlesticks", "raw_kalshi_candlesticks"),
                ):
                    ck = _canonical_raw_hourly_key(
                        dataset=dataset,
                        partition_start=partition_start,
                    )
                    prefix = partition_path("raw", dataset, run_date)
                    ds_keys = set(list_jsonl_keys_under(prefix, bucket=bucket))
                    if ck not in ds_keys:
                        ok = False
                        break
                    canonical_row[field] = ck
                if ok and len(canonical_row) == 6:
                    canonical_row["source_batch_id"] = (
                        f"{partition_start.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_canonical"
                    )
                    canonical_row["source_window_start"] = ""
                    canonical_row["source_window_end"] = ""
                    return ([canonical_row], bucket)
            micro_markets = sorted(
                k for k in keys if k.endswith("_markets.jsonl") and "/batch_id=" in k
            )
            out: list[dict[str, str]] = []
            for mk in micro_markets:
                ts = _batch_ts_from_key(mk)
                if ts is None or not (partition_start <= ts < partition_end):
                    continue
                parts = _kalshi_micro_keys_from_markets_key(mk)
                if any(pk not in keys for pk in parts.values()):
                    continue
                parts["source_batch_id"] = _batch_id_from_micro_key(mk)
                parts["source_window_start"] = ""
                parts["source_window_end"] = ""
                out.append(parts)
            return out, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(
        _no_raw_window_msg(
            dataset="provider=kalshi",
            partition_start=partition_start,
            partition_end=partition_end,
        )
    )


def list_raw_fear_greed_for_partition_window(
    *,
    partition_start: datetime,
    partition_end: datetime,
    include_canonical_hourly: bool = False,
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    dataset = "provider=alternative_me_fear_greed"
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys(dataset, partition_start, partition_end, bucket=bucket)
            if include_canonical_hourly:
                canonical_key = _canonical_raw_hourly_key(
                    dataset=dataset,
                    partition_start=partition_start,
                )
                if canonical_key in keys:
                    canonical_batch_id = (
                        f"{partition_start.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_canonical"
                    )
                    return (
                        [
                            {
                                "raw_fear_greed_payload": canonical_key,
                                "source_batch_id": canonical_batch_id,
                                "source_window_start": "",
                                "source_window_end": "",
                            }
                        ],
                        bucket,
                    )
            micro = sorted(k for k in keys if _is_microbatch_non_polymarket_jsonl(k))
            out: list[dict[str, str]] = []
            for chosen in micro:
                ts = _batch_ts_from_key(chosen)
                if ts is None or not (partition_start <= ts < partition_end):
                    continue
                out.append(
                    {
                        "raw_fear_greed_payload": chosen,
                        "source_batch_id": _batch_id_from_micro_key(chosen),
                        "source_window_start": "",
                        "source_window_end": "",
                    }
                )
            return out, bucket
        except FileNotFoundError as e:
            last_err = e
            continue
    raise last_err or FileNotFoundError(
        _no_raw_window_msg(
            dataset=dataset,
            partition_start=partition_start,
            partition_end=partition_end,
        )
    )


def unseen_raw_inputs_for_partition(
    *,
    consumer_asset: str,
    source: str,
    partition_key: str,
    candidate_inputs: list[dict[str, str]],
    key_fields: tuple[str, ...],
) -> list[dict[str, str]]:
    wm = read_processing_watermark(
        consumer_asset=consumer_asset,
        source=source,
        partition_key=partition_key,
    )
    if wm is None or not wm.last_processed_batch_id:
        return _dedupe_candidates(candidate_inputs, key_fields)
    watermark_batch_id = wm.last_processed_batch_id
    watermark_ts = _batch_ts_from_batch_id(watermark_batch_id)
    if watermark_ts is None:
        # Old or malformed watermarks are not safe to compare. Reprocess
        # candidates deterministically rather than silently skipping new input.
        return _dedupe_candidates(candidate_inputs, key_fields)
    unseen: list[dict[str, str]] = []
    for item in candidate_inputs:
        bid = item.get("source_batch_id") or ""
        if _is_batch_newer_than_watermark(
            bid,
            watermark_batch_id=watermark_batch_id,
            watermark_ts=watermark_ts,
        ):
            unseen.append(item)
    if not unseen:
        return []
    return _dedupe_candidates(unseen, key_fields)


__all__ = [
    "list_raw_binance_for_partition_window",
    "list_raw_fear_greed_for_partition_window",
    "list_raw_gdelt_for_partition_window",
    "list_raw_kalshi_for_partition_window",
    "list_raw_polymarket_for_partition_window",
    "resolve_raw_binance_for_partition",
    "resolve_raw_fear_greed_for_partition",
    "resolve_raw_gdelt_for_partition",
    "resolve_raw_polymarket_for_partition",
    "unseen_raw_inputs_for_partition",
]
