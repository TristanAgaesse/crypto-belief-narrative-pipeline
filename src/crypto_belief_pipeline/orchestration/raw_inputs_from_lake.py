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
    for suf in ("_markets.jsonl", "_prices.jsonl", ".jsonl"):
        if segment.endswith(suf):
            return segment[: -len(suf)]
    return segment


def _batch_ts_from_key(key: str) -> datetime | None:
    bid = _batch_id_from_micro_key(key)
    if not bid:
        return None
    core = bid.split("_", 1)[0]
    try:
        return datetime.strptime(core, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


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


def _list_window_jsonl_keys(dataset: str, partition_start: datetime, partition_end: datetime, *, bucket: str) -> set[str]:
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
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys("provider=polymarket", partition_start, partition_end, bucket=bucket)
            micro_markets = sorted(k for k in keys if k.endswith("_markets.jsonl") and "/batch_id=" in k)
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
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys("provider=binance", partition_start, partition_end, bucket=bucket)
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
) -> tuple[list[dict[str, str]], str]:
    last_err: FileNotFoundError | None = None
    for bucket in _candidate_buckets():
        try:
            keys = _list_window_jsonl_keys("provider=gdelt", partition_start, partition_end, bucket=bucket)
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
        return candidate_inputs
    unseen: list[dict[str, str]] = []
    for item in candidate_inputs:
        bid = item.get("source_batch_id") or ""
        if bid > wm.last_processed_batch_id:
            unseen.append(item)
    # Safety fallback for old watermarks/malformed ids: keep deterministic order.
    if not unseen:
        return []
    # De-duplicate by key fields.
    seen: set[tuple[str, ...]] = set()
    out: list[dict[str, str]] = []
    for item in unseen:
        sig = tuple(str(item.get(k, "")) for k in key_fields)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(item)
    return out


__all__ = [
    "list_raw_binance_for_partition_window",
    "list_raw_gdelt_for_partition_window",
    "list_raw_polymarket_for_partition_window",
    "resolve_raw_binance_for_partition",
    "resolve_raw_gdelt_for_partition",
    "resolve_raw_polymarket_for_partition",
    "unseen_raw_inputs_for_partition",
]
