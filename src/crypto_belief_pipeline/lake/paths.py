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
