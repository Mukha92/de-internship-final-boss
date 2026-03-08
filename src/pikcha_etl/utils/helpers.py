"""Вспомогательные утилиты для нормализации данных."""

from __future__ import annotations

import re
from typing import Any


def normalize_phone(phone: Any) -> str:
    """
    Нормализует номер телефона в формат ``+7XXXXXXXXXX``.

    Args:
        phone: Номер телефона в произвольном строковом формате.

    Returns:
        Нормализованный номер телефона или пустую строку,
        если входное значение пустое.
    """
    if not phone:
        return str(phone) if phone else ""

    phone_str = re.sub(r"[^\d+]", "", str(phone).strip())

    if phone_str.startswith("8"):
        return "+7" + phone_str[1:]
    if phone_str.startswith("7") and "+" not in phone_str:
        return "+7" + phone_str[1:]
    if len(phone_str) == 10:
        return "+7" + phone_str

    return phone_str


def normalize_email(email: Any) -> str:
    """
    Нормализует email-адрес.

    Приводит значение к строке, обрезает пробелы и приводит к нижнему регистру.

    Args:
        email: Email-адрес в произвольном формате.

    Returns:
        Нормализованный email или пустую строку,
        если входное значение пустое.
    """
    if not email:
        return str(email) if email else ""
    return str(email).strip().lower()
