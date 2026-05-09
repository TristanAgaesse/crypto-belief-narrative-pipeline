"""Normalize Kalshi raw envelopes into bronze/silver Polars frames + relevance scoring."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import polars as pl

_TS_FORMAT_Z = "%Y-%m-%dT%H:%M:%SZ"
_TS_FORMAT_FRAC = "%Y-%m-%dT%H:%M:%S%.fZ"


def _parse_utc_any(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.String)
    parsed_z = s.str.strptime(pl.Datetime, format=_TS_FORMAT_Z, strict=False)
    parsed_f = s.str.strptime(pl.Datetime, format=_TS_FORMAT_FRAC, strict=False)
    return (
        pl.when(s.is_null() | (s.str.strip_chars() == ""))
        .then(pl.lit(None, dtype=pl.Datetime(time_zone="UTC")))
        .otherwise(pl.coalesce([parsed_f, parsed_z]).dt.replace_time_zone("UTC"))
    )


def _ingested_at() -> pl.Expr:
    return pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("ingested_at")


def _kalshi_envelope_body(r: dict[str, Any]) -> dict[str, Any]:
    inner = r.get("data")
    if isinstance(inner, dict):
        return inner
    return r


def _kalshi_envelope_data_dict(r: dict[str, Any]) -> dict[str, Any]:
    inner = r.get("data")
    if isinstance(inner, dict):
        return inner
    return {}


def _raw_json_from_envelopes(records: list[dict]) -> list[str]:
    return [json.dumps(r, ensure_ascii=False) for r in records]


def _dollars_to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return None


def load_keyword_config(path: str = "config/kalshi_keywords.yaml") -> dict[str, Any]:
    try:
        import yaml  # type: ignore[import-untyped]

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def classify_kalshi_market_priority(
    market: dict[str, Any],
    event: dict[str, Any] | None,
    series: dict[str, Any] | None,
    *,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Return relevance_label, priority (int), mapped_assets (list), reason_codes (list)."""

    keywords = cfg.get("keywords") or []
    needles = (
        [str(k).strip().lower() for k in keywords if str(k).strip()]
        if isinstance(keywords, list)
        else []
    )

    hay_parts: list[str] = []
    for key in ("ticker", "yes_sub_title", "no_sub_title", "title", "subtitle", "event_ticker"):
        v = market.get(key)
        if isinstance(v, str):
            hay_parts.append(v)
    if event:
        for key in ("title", "sub_title", "event_ticker", "category"):
            v = event.get(key)
            if isinstance(v, str):
                hay_parts.append(v)
    if series:
        for key in ("ticker", "title", "category", "frequency"):
            v = series.get(key)
            if isinstance(v, str):
                hay_parts.append(v)
    hay = " ".join(hay_parts).lower()

    matched_kw = [n for n in needles if n and n in hay]
    aliases = cfg.get("asset_aliases") or {}
    mapped: list[str] = []
    reason_codes: list[str] = []
    if isinstance(aliases, dict):
        for asset, pats in aliases.items():
            if not isinstance(pats, list):
                continue
            asset_u = str(asset).upper()
            for p in pats:
                sub = str(p).lower()
                if sub and sub in hay:
                    if asset_u not in mapped:
                        mapped.append(asset_u)
                    reason_codes.append(f"asset_alias:{asset_u}")
                    break

    if matched_kw:
        relevance = "crypto_relevant"
        reason_codes.append("keyword_hit")
        priority = 2 if mapped else 1
    else:
        relevance = "other"
        priority = 0 if not mapped else 1

    return {
        "relevance_label": relevance,
        "priority": priority,
        "mapped_assets": mapped,
        "reason_codes": sorted(set(reason_codes)),
        "matched_keywords": matched_kw,
    }


def normalize_kalshi_markets(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "market_ticker": pl.String,
                "event_ticker": pl.String,
                "series_ticker": pl.String,
                "status": pl.String,
                "market_type": pl.String,
                "fetched_at": pl.Datetime,
                "snapshot_time": pl.Datetime,
                "yes_bid": pl.Float64,
                "yes_ask": pl.Float64,
                "last_price": pl.Float64,
                "volume_fp": pl.String,
                "volume_24h_fp": pl.String,
                "open_interest_fp": pl.String,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )

    slim: list[dict[str, Any]] = []
    for r in records:
        data = _kalshi_envelope_body(r)
        slim.append(
            {
                "market_ticker": data.get("ticker"),
                "event_ticker": data.get("event_ticker"),
                "series_ticker": data.get("series_ticker") or data.get("mve_collection_ticker"),
                "status": data.get("status"),
                "market_type": data.get("market_type"),
                "fetched_at": r.get("fetched_at"),
                "snapshot_time": r.get("snapshot_time"),
                "yes_bid_dollars": data.get("yes_bid_dollars"),
                "yes_ask_dollars": data.get("yes_ask_dollars"),
                "last_price_dollars": data.get("last_price_dollars"),
                "volume_fp": data.get("volume_fp"),
                "volume_24h_fp": data.get("volume_24h_fp"),
                "open_interest_fp": data.get("open_interest_fp"),
            }
        )
    df = pl.DataFrame(slim)
    bronze = df.select(
        pl.col("market_ticker").cast(pl.String),
        pl.col("event_ticker").cast(pl.String),
        pl.col("series_ticker").cast(pl.String),
        pl.col("status").cast(pl.String),
        pl.col("market_type").cast(pl.String),
        _parse_utc_any("fetched_at").alias("fetched_at"),
        _parse_utc_any("snapshot_time").alias("snapshot_time"),
        pl.col("yes_bid_dollars")
        .map_elements(_dollars_to_float, return_dtype=pl.Float64)
        .alias("yes_bid"),
        pl.col("yes_ask_dollars")
        .map_elements(_dollars_to_float, return_dtype=pl.Float64)
        .alias("yes_ask"),
        pl.col("last_price_dollars")
        .map_elements(_dollars_to_float, return_dtype=pl.Float64)
        .alias("last_price"),
        pl.col("volume_fp").cast(pl.String),
        pl.col("volume_24h_fp").cast(pl.String),
        pl.col("open_interest_fp").cast(pl.String),
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
    ).with_columns(_ingested_at())
    bronze = bronze.sort(["market_ticker", "fetched_at"]).unique(
        subset=["market_ticker"], keep="last"
    )
    return bronze


def normalize_kalshi_events(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "event_ticker": pl.String,
                "series_ticker": pl.String,
                "title": pl.String,
                "sub_title": pl.String,
                "category": pl.String,
                "fetched_at": pl.Datetime,
                "snapshot_time": pl.Datetime,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )
    slim = []
    for r in records:
        d = _kalshi_envelope_body(r)
        slim.append(
            {
                "event_ticker": d.get("event_ticker"),
                "series_ticker": d.get("series_ticker"),
                "title": d.get("title"),
                "sub_title": d.get("sub_title"),
                "category": d.get("category"),
                "fetched_at": r.get("fetched_at"),
                "snapshot_time": r.get("snapshot_time"),
            }
        )
    df = pl.DataFrame(slim)
    bronze = df.select(
        pl.col("event_ticker").cast(pl.String),
        pl.col("series_ticker").cast(pl.String),
        pl.col("title").cast(pl.String),
        pl.col("sub_title").cast(pl.String),
        pl.col("category").cast(pl.String),
        _parse_utc_any("fetched_at").alias("fetched_at"),
        _parse_utc_any("snapshot_time").alias("snapshot_time"),
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
    ).with_columns(_ingested_at())
    bronze = bronze.sort(["event_ticker", "fetched_at"]).unique(
        subset=["event_ticker"], keep="last"
    )
    return bronze


def normalize_kalshi_series(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "series_ticker": pl.String,
                "title": pl.String,
                "category": pl.String,
                "frequency": pl.String,
                "fetched_at": pl.Datetime,
                "snapshot_time": pl.Datetime,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )
    slim = []
    for r in records:
        d = _kalshi_envelope_body(r)
        slim.append(
            {
                "series_ticker": d.get("ticker"),
                "title": d.get("title"),
                "category": d.get("category"),
                "frequency": d.get("frequency"),
                "fetched_at": r.get("fetched_at"),
                "snapshot_time": r.get("snapshot_time"),
            }
        )
    df = pl.DataFrame(slim)
    bronze = df.select(
        pl.col("series_ticker").cast(pl.String),
        pl.col("title").cast(pl.String),
        pl.col("category").cast(pl.String),
        pl.col("frequency").cast(pl.String),
        _parse_utc_any("fetched_at").alias("fetched_at"),
        _parse_utc_any("snapshot_time").alias("snapshot_time"),
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
    ).with_columns(_ingested_at())
    bronze = bronze.sort(["series_ticker", "fetched_at"]).unique(
        subset=["series_ticker"], keep="last"
    )
    return bronze


def normalize_kalshi_trades(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "trade_id": pl.String,
                "market_ticker": pl.String,
                "executed_at": pl.Datetime,
                "taker_side": pl.String,
                "yes_price": pl.Float64,
                "no_price": pl.Float64,
                "size_fp": pl.String,
                "dedupe_key": pl.String,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )
    slim = []
    for r in records:
        d = _kalshi_envelope_body(r)
        tid = d.get("trade_id")
        mkt = d.get("ticker")
        created = d.get("created_time")
        yes_p = d.get("yes_price_dollars")
        no_p = d.get("no_price_dollars")
        cnt = d.get("count_fp")
        tid_s = str(tid) if tid is not None else ""
        fallback = hashlib.sha256(f"{mkt}|{created}|{yes_p}|{no_p}|{cnt}".encode()).hexdigest()[:32]
        dedupe = tid_s or fallback
        slim.append(
            {
                "trade_id": tid_s or None,
                "market_ticker": mkt,
                "executed_at": created,
                "taker_side": d.get("taker_side"),
                "yes_price_dollars": yes_p,
                "no_price_dollars": no_p,
                "count_fp": cnt,
                "dedupe_key": dedupe,
            }
        )
    df = pl.DataFrame(slim)
    bronze = df.select(
        pl.col("trade_id").cast(pl.String),
        pl.col("market_ticker").cast(pl.String),
        _parse_utc_any("executed_at").alias("executed_at"),
        pl.col("taker_side").cast(pl.String),
        pl.col("yes_price_dollars")
        .map_elements(_dollars_to_float, return_dtype=pl.Float64)
        .alias("yes_price"),
        pl.col("no_price_dollars")
        .map_elements(_dollars_to_float, return_dtype=pl.Float64)
        .alias("no_price"),
        pl.col("count_fp").cast(pl.String).alias("size_fp"),
        pl.col("dedupe_key").cast(pl.String),
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
    ).with_columns(_ingested_at())
    bronze = bronze.sort(["dedupe_key", "executed_at"]).unique(subset=["dedupe_key"], keep="last")
    return bronze


def normalize_kalshi_orderbooks(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "market_ticker": pl.String,
                "fetched_at": pl.Datetime,
                "snapshot_time": pl.Datetime,
                "best_yes_bid": pl.Float64,
                "best_yes_ask": pl.Float64,
                "mid_probability": pl.Float64,
                "spread": pl.Float64,
                "depth_yes_top": pl.Float64,
                "depth_no_top": pl.Float64,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )
    rows: list[dict[str, Any]] = []
    for r in records:
        outer = _kalshi_envelope_data_dict(r)
        m_tk = outer.get("market_ticker")
        raw_ob = outer.get("orderbook")
        ob: dict[str, Any] = raw_ob if isinstance(raw_ob, dict) else {}
        raw_fp = ob.get("orderbook_fp")
        ob_fp: dict[str, Any] = raw_fp if isinstance(raw_fp, dict) else {}
        yes_levels = ob_fp.get("yes_dollars") or []
        no_levels = ob_fp.get("no_dollars") or []
        best_yes_bid = None
        depth_yes = 0.0
        if isinstance(yes_levels, list) and yes_levels:
            lvl = yes_levels[0]
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                best_yes_bid = _dollars_to_float(lvl[0])
                depth_yes = float(_dollars_to_float(lvl[1]) or 0.0)
        best_no_bid = None
        depth_no = 0.0
        if isinstance(no_levels, list) and no_levels:
            lvl = no_levels[0]
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                best_no_bid = _dollars_to_float(lvl[0])
                depth_no = float(_dollars_to_float(lvl[1]) or 0.0)
        best_yes_ask = (1.0 - best_no_bid) if best_no_bid is not None else None
        mid = (
            (best_yes_bid + best_yes_ask) / 2.0
            if best_yes_bid is not None and best_yes_ask is not None
            else None
        )
        spread = (
            (best_yes_ask - best_yes_bid)
            if best_yes_bid is not None and best_yes_ask is not None
            else None
        )
        rows.append(
            {
                "market_ticker": m_tk,
                "fetched_at": r.get("fetched_at"),
                "snapshot_time": r.get("snapshot_time"),
                "best_yes_bid": best_yes_bid,
                "best_yes_ask": best_yes_ask,
                "mid_probability": mid,
                "spread": spread,
                "depth_yes_top": depth_yes,
                "depth_no_top": depth_no,
            }
        )
    df = pl.DataFrame(rows)
    bronze = df.select(
        pl.col("market_ticker").cast(pl.String),
        _parse_utc_any("fetched_at").alias("fetched_at"),
        _parse_utc_any("snapshot_time").alias("snapshot_time"),
        pl.col("best_yes_bid").cast(pl.Float64),
        pl.col("best_yes_ask").cast(pl.Float64),
        pl.col("mid_probability").cast(pl.Float64),
        pl.col("spread").cast(pl.Float64),
        pl.col("depth_yes_top").cast(pl.Float64),
        pl.col("depth_no_top").cast(pl.Float64),
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
    ).with_columns(_ingested_at())
    mt = pl.col("market_ticker").cast(pl.String)
    fa = pl.col("fetched_at").cast(pl.String)
    key = (mt + "|" + fa).hash()
    bronze = (
        bronze.with_columns(key.alias("_row_key"))
        .sort(["_row_key"])
        .unique(subset=["market_ticker", "fetched_at"], keep="last")
        .drop("_row_key")
    )
    return bronze


def normalize_kalshi_candlesticks(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "market_ticker": pl.String,
                "interval": pl.String,
                "period_start": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )
    rows: list[dict[str, Any]] = []
    for r in records:
        outer = _kalshi_envelope_data_dict(r)
        m_tk = outer.get("market_ticker")
        interval = str(outer.get("period_minutes") or "")
        raw_c = outer.get("candle")
        c: dict[str, Any] = raw_c if isinstance(raw_c, dict) else {}
        end_period_ts = c.get("end_period_ts") or c.get("start_period_ts")
        rows.append(
            {
                "market_ticker": m_tk,
                "interval": interval,
                "end_period_ts": end_period_ts,
                "open": c.get("open_dollars") or c.get("open"),
                "high": c.get("high_dollars") or c.get("high"),
                "low": c.get("low_dollars") or c.get("low"),
                "close": c.get("close_dollars") or c.get("close"),
                "volume": c.get("volume") or c.get("volume_fp"),
            }
        )
    df = pl.DataFrame(rows)
    bronze = df.select(
        pl.col("market_ticker").cast(pl.String),
        pl.col("interval").cast(pl.String),
        pl.col("end_period_ts").cast(pl.Int64, strict=False).alias("_end_ts"),
        pl.col("open").map_elements(_dollars_to_float, return_dtype=pl.Float64),
        pl.col("high").map_elements(_dollars_to_float, return_dtype=pl.Float64),
        pl.col("low").map_elements(_dollars_to_float, return_dtype=pl.Float64),
        pl.col("close").map_elements(_dollars_to_float, return_dtype=pl.Float64),
        pl.col("volume").map_elements(_dollars_to_float, return_dtype=pl.Float64),
    )
    bronze = bronze.with_columns(
        pl.when(pl.col("_end_ts").is_not_null())
        .then((pl.col("_end_ts") * 1_000_000).cast(pl.Datetime("us")).dt.replace_time_zone("UTC"))
        .otherwise(None)
        .alias("period_start")
    ).drop("_end_ts")
    bronze = bronze.with_columns(
        pl.Series("raw_json", _raw_json_from_envelopes(records)),
        _ingested_at(),
    )
    bronze = bronze.sort(["market_ticker", "interval", "period_start"]).unique(
        subset=["market_ticker", "interval", "period_start"], keep="last"
    )
    return bronze


def _event_lookup(events_df: pl.DataFrame) -> dict[str, dict]:
    if events_df.height == 0:
        return {}
    out: dict[str, dict] = {}
    for row in events_df.iter_rows(named=True):
        tk = row.get("event_ticker")
        if isinstance(tk, str) and tk:
            out[tk] = row
    return out


def _series_lookup(series_df: pl.DataFrame) -> dict[str, dict]:
    if series_df.height == 0:
        return {}
    out: dict[str, dict] = {}
    for row in series_df.iter_rows(named=True):
        tk = row.get("series_ticker")
        if isinstance(tk, str) and tk:
            out[tk] = row
    return out


def to_silver_kalshi_markets(
    markets_bronze: pl.DataFrame,
    events_bronze: pl.DataFrame,
    series_bronze: pl.DataFrame,
    *,
    cfg: dict[str, Any],
) -> pl.DataFrame:
    if markets_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "fetched_at": pl.Datetime,
                "market_ticker": pl.String,
                "event_ticker": pl.String,
                "series_ticker": pl.String,
                "status": pl.String,
                "market_type": pl.String,
                "yes_bid": pl.Float64,
                "yes_ask": pl.Float64,
                "last_price": pl.Float64,
                "relevance_label": pl.String,
                "priority": pl.Int64,
                "mapped_assets": pl.String,
                "reason_codes": pl.String,
            }
        )
    ev = _event_lookup(events_bronze)
    se = _series_lookup(series_bronze)
    rows: list[dict[str, Any]] = []
    for row in markets_bronze.iter_rows(named=True):
        m = dict(row)
        e_tk = m.get("event_ticker")
        s_tk = m.get("series_ticker")
        event = ev.get(str(e_tk)) if e_tk else None
        series = se.get(str(s_tk)) if s_tk else None
        api_market: dict[str, Any] = {}
        rawj = m.get("raw_json")
        if isinstance(rawj, str):
            try:
                envelope = json.loads(rawj)
                if isinstance(envelope.get("data"), dict):
                    api_market = envelope["data"]
            except (TypeError, ValueError):
                api_market = {}
        cls = classify_kalshi_market_priority(
            api_market if api_market else m,
            event,
            series,
            cfg=cfg,
        )
        rows.append(
            {
                "fetched_at": m.get("fetched_at"),
                "market_ticker": m.get("market_ticker"),
                "event_ticker": m.get("event_ticker"),
                "series_ticker": m.get("series_ticker"),
                "status": m.get("status"),
                "market_type": m.get("market_type"),
                "yes_bid": m.get("yes_bid"),
                "yes_ask": m.get("yes_ask"),
                "last_price": m.get("last_price"),
                "relevance_label": cls["relevance_label"],
                "priority": cls["priority"],
                "mapped_assets": ",".join(cls["mapped_assets"]),
                "reason_codes": ",".join(cls["reason_codes"]),
            }
        )
    return pl.DataFrame(rows)


def to_silver_kalshi_market_snapshots(markets_bronze: pl.DataFrame) -> pl.DataFrame:
    if markets_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "fetched_at": pl.Datetime,
                "market_ticker": pl.String,
                "event_ticker": pl.String,
                "best_yes_bid": pl.Float64,
                "best_yes_ask": pl.Float64,
                "last_price": pl.Float64,
                "mid_probability": pl.Float64,
                "spread": pl.Float64,
            }
        )
    df = markets_bronze.select(
        pl.col("fetched_at"),
        pl.col("market_ticker"),
        pl.col("event_ticker"),
        pl.col("yes_bid").alias("best_yes_bid"),
        pl.col("yes_ask").alias("best_yes_ask"),
        pl.col("last_price"),
        pl.when(pl.col("yes_bid").is_not_null() & pl.col("yes_ask").is_not_null())
        .then((pl.col("yes_bid") + pl.col("yes_ask")) / 2.0)
        .otherwise(pl.col("last_price"))
        .alias("mid_probability"),
        pl.when(pl.col("yes_bid").is_not_null() & pl.col("yes_ask").is_not_null())
        .then(pl.col("yes_ask") - pl.col("yes_bid"))
        .otherwise(None)
        .alias("spread"),
    )
    return df.sort(["market_ticker", "fetched_at"]).unique(
        subset=["market_ticker", "fetched_at"], keep="last"
    )


def to_silver_kalshi_events(events_bronze: pl.DataFrame) -> pl.DataFrame:
    if events_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "fetched_at": pl.Datetime,
                "event_ticker": pl.String,
                "series_ticker": pl.String,
                "title": pl.String,
                "sub_title": pl.String,
                "category": pl.String,
            }
        )
    return events_bronze.select(
        pl.col("fetched_at"),
        pl.col("event_ticker"),
        pl.col("series_ticker"),
        pl.col("title"),
        pl.col("sub_title"),
        pl.col("category"),
    ).with_columns(
        pl.col("title").fill_null(""),
        pl.col("category").fill_null(""),
        pl.col("series_ticker").fill_null(""),
    )


def to_silver_kalshi_series(series_bronze: pl.DataFrame) -> pl.DataFrame:
    if series_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "fetched_at": pl.Datetime,
                "series_ticker": pl.String,
                "title": pl.String,
                "category": pl.String,
                "frequency": pl.String,
            }
        )
    return series_bronze.select(
        pl.col("fetched_at"),
        pl.col("series_ticker"),
        pl.col("title"),
        pl.col("category"),
        pl.col("frequency"),
    ).with_columns(
        pl.col("title").fill_null(""),
        pl.col("category").fill_null(""),
        pl.col("frequency").fill_null(""),
    )


def to_silver_kalshi_trades(trades_bronze: pl.DataFrame) -> pl.DataFrame:
    if trades_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "executed_at": pl.Datetime,
                "market_ticker": pl.String,
                "trade_id": pl.String,
                "taker_side": pl.String,
                "yes_price": pl.Float64,
                "no_price": pl.Float64,
                "size_fp": pl.String,
            }
        )
    return trades_bronze.select(
        pl.col("executed_at"),
        pl.col("market_ticker"),
        pl.coalesce(pl.col("trade_id"), pl.col("dedupe_key")).cast(pl.String).alias("trade_id"),
        pl.col("taker_side"),
        pl.col("yes_price"),
        pl.col("no_price"),
        pl.col("size_fp"),
    )


def to_silver_kalshi_orderbook_snapshots(orderbooks_bronze: pl.DataFrame) -> pl.DataFrame:
    if orderbooks_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "fetched_at": pl.Datetime,
                "market_ticker": pl.String,
                "best_yes_bid": pl.Float64,
                "best_yes_ask": pl.Float64,
                "mid_probability": pl.Float64,
                "spread": pl.Float64,
                "depth_yes_top": pl.Float64,
                "depth_no_top": pl.Float64,
            }
        )
    return orderbooks_bronze.select(
        pl.col("fetched_at"),
        pl.col("market_ticker"),
        pl.col("best_yes_bid"),
        pl.col("best_yes_ask"),
        pl.col("mid_probability"),
        pl.col("spread"),
        pl.col("depth_yes_top"),
        pl.col("depth_no_top"),
    )


def to_silver_kalshi_candlesticks(candles_bronze: pl.DataFrame) -> pl.DataFrame:
    if candles_bronze.height == 0:
        return pl.DataFrame(
            schema={
                "market_ticker": pl.String,
                "interval": pl.String,
                "period_start": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            }
        )
    return candles_bronze.select(
        pl.col("market_ticker"),
        pl.col("interval"),
        pl.col("period_start"),
        pl.col("open"),
        pl.col("high"),
        pl.col("low"),
        pl.col("close"),
        pl.col("volume"),
    ).with_columns(pl.col("close").fill_null(0.0))


__all__ = [
    "classify_kalshi_market_priority",
    "load_keyword_config",
    "normalize_kalshi_candlesticks",
    "normalize_kalshi_events",
    "normalize_kalshi_markets",
    "normalize_kalshi_orderbooks",
    "normalize_kalshi_series",
    "normalize_kalshi_trades",
    "to_silver_kalshi_candlesticks",
    "to_silver_kalshi_events",
    "to_silver_kalshi_market_snapshots",
    "to_silver_kalshi_markets",
    "to_silver_kalshi_orderbook_snapshots",
    "to_silver_kalshi_series",
    "to_silver_kalshi_trades",
]
