from __future__ import annotations

from typing import Any


def get_value(document: dict[str, Any], path: str) -> Any:
    current: Any = document
    for part in _parts(path):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue
        return None
    return current


def set_value(document: dict[str, Any], path: str, value: Any) -> None:
    parts = _parts(path)
    if not parts:
        raise ValueError("path cannot be empty")

    current: Any = document
    for part in parts[:-1]:
        if isinstance(current, dict):
            next_value = current[part] if part in current else None
            if not isinstance(next_value, dict):
                next_value = {}
                current[part] = next_value
            current = next_value
            continue
        raise TypeError(f"cannot descend through non-dict object at '{part}'")

    if not isinstance(current, dict):
        raise TypeError("target parent must be a dict")
    current[parts[-1]] = value


def _parts(path: str) -> list[str]:
    parts = [part.strip() for part in path.split(".") if part.strip()]
    if not parts:
        raise ValueError("path cannot be empty")
    return parts
