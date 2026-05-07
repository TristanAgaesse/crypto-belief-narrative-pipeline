"""Resolve raw JSONL lake keys for a daily partition without Dagster IO-manager inputs.

Bronze assets depend on ``raw_*`` for lineage, but when jobs omit the raw assets
(e.g. ``silver_microbatch_5m_job``), Dagster cannot load upstream outputs from
ephemeral run storage. These helpers list objects under the lake prefix and pick
the same keys a co-located raw collector would emit (microbatch, CLI ``live_*``,
or ``sample_*`` layout).
"""

from __future__ import annotations

from datetime import date

from crypto_belief_pipeline.config import get_runtime_config, get_settings
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import list_jsonl_keys_under


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


def _is_microbatch_non_polymarket_jsonl(k: str) -> bool:
    return (
        "/batch_id=" in k
        and k.endswith(".jsonl")
        and "_markets" not in k
        and "_prices" not in k
    )


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


__all__ = [
    "resolve_raw_binance_for_partition",
    "resolve_raw_gdelt_for_partition",
    "resolve_raw_polymarket_for_partition",
]
