from datetime import date, datetime

_VALID_LAYERS = {"raw", "bronze", "silver", "gold"}


def _to_date(dt: date | datetime | str) -> date:
    if isinstance(dt, datetime):
        return dt.date()
    if isinstance(dt, date):
        return dt
    if isinstance(dt, str):
        return date.fromisoformat(dt)
    raise TypeError(f"Unsupported dt type: {type(dt)!r}")


def partition_path(layer: str, dataset: str, dt: date | datetime | str) -> str:
    if layer not in _VALID_LAYERS:
        raise ValueError(f"Invalid layer: {layer!r}. Expected one of {sorted(_VALID_LAYERS)}")
    d = _to_date(dt)
    return f"{layer}/{dataset}/date={d.isoformat()}"


def microbatch_dir(
    layer: str,
    dataset: str,
    dt: date | datetime | str,
    hour: int | str,
) -> str:
    if layer not in _VALID_LAYERS:
        raise ValueError(f"Invalid layer: {layer!r}. Expected one of {sorted(_VALID_LAYERS)}")
    d = _to_date(dt)
    hh = int(hour)
    if hh < 0 or hh > 23:
        raise ValueError(f"Invalid hour: {hour!r}. Expected 0-23")
    return f"{layer}/{dataset}/date={d.isoformat()}/hour={hh:02d}"


def microbatch_key(
    layer: str,
    dataset: str,
    dt: date | datetime | str,
    hour: int | str,
    batch_id: str,
    suffix: str,
) -> str:
    base = microbatch_dir(layer=layer, dataset=dataset, dt=dt, hour=hour)
    suffix = suffix.lstrip("/")
    return f"{base}/batch_id={batch_id}{suffix}"
