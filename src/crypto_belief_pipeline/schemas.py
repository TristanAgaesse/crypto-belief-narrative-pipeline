from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

Layer = Literal["raw", "bronze", "silver", "gold"]


class JsonlRecord(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)
