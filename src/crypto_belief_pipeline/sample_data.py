from __future__ import annotations

import json
from pathlib import Path


def _repo_root() -> Path:
    # `.../crypto-belief-narrative-pipeline/src/crypto_belief_pipeline/sample_data.py`
    # Repo root is two levels above `src/`.
    return Path(__file__).resolve().parents[2]


def sample_file_path(name: str) -> Path:
    p = _repo_root() / "data" / "sample" / name
    if not p.exists():
        raise FileNotFoundError(f"Sample file not found: {p}")
    return p


def load_sample_jsonl(name: str) -> list[dict]:
    p = sample_file_path(name)
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:  # pragma: no cover
                raise ValueError(f"Invalid JSON in {p} line {i}: {e}") from e
            if not isinstance(obj, dict):
                raise TypeError(f"Expected JSON object (dict) in {p} line {i}, got {type(obj)!r}")
            out.append(obj)
    return out
