"""Утилиты для хеширования конфиденциальных данных."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any


def generate_hmac_hash(value: Any, secret_key: str) -> str:
    """
    Хеширует значение с использованием алгоритма HMAC-SHA256.

    Args:
        value: Произвольное значение, которое нужно захешировать.
        secret_key: Секретный ключ для HMAC.

    Returns:
        Hex-представление HMAC-SHA256 хеша. Если ``value`` пустое,
        возвращается его строковое представление или пустая строка.
    """
    if not value:
        return str(value) if value else ""

    data = str(value).encode("utf-8")
    key = secret_key.encode("utf-8")
    return hmac.new(key, data, hashlib.sha256).hexdigest()
