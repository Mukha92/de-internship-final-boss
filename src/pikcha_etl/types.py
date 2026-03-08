"""Общие типы и алиасы для проекта Pikcha ETL."""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeAlias

JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]

StrPath: TypeAlias = str | Path

StringMap: TypeAlias = dict[str, str]
StrAnyMap: TypeAlias = dict[str, Any]

