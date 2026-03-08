"""Централизованная конфигурация логирования для проекта."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from src.pikcha_etl.types import StringMap

LOG_DIR: Path = Path(__file__).resolve().parent.parent / "logs"


def _build_handlers(log_file_name: Optional[str]) -> list[logging.Handler]:
    """Создаёт набор хендлеров для логирования (консоль + файл)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
    ]

    file_name = log_file_name or "app.log"
    file_path = LOG_DIR / file_name
    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    handlers.append(file_handler)

    return handlers


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    include_module: bool = True,
    log_file_name: Optional[str] = None,
) -> None:
    """
    Настраивает единое логирование для всего проекта.

    Логи пишутся одновременно в stdout и в файл в директории ``logs``.

    Args:
        level: Уровень логирования (по умолчанию ``logging.INFO``).
        format_string: Явно заданный формат логов. Если не указан,
            собирается автоматически на основе ``include_module``.
        include_module: Включать ли имя логгера/модуля в сообщение.
        log_file_name: Имя файла лога в папке ``logs``. Если не указано,
            используется ``app.log``.
    """
    if format_string is None:
        if include_module:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        else:
            format_string = "%(asctime)s - %(levelname)s - %(message)s"

    handlers = _build_handlers(log_file_name)

    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Возвращает заранее сконфигурированный логгер для модуля.

    Args:
        name: Имя логгера (обычно ``__name__``).

    Returns:
        Экземпляр ``logging.Logger`` с уже настроенными хендлерами.
    """
    return logging.getLogger(name)


def get_log_paths() -> StringMap:
    """
    Возвращает словарь с основными путями до лог-файлов.

    Returns:
        Словарь с ключами ``"root"`` и ``"directory"``.
    """
    return {
        "root": str(LOG_DIR / "app.log"),
        "directory": str(LOG_DIR),
    }
