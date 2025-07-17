#!/usr/bin/env python3
"""
Stream‑only Loguru configuration
===============================
Выводит логи **только в консоль** (stderr). Никаких файлов, ротаций и прочего.

Использование
-------------
    from src.sdk.infrastructure.logger import logger
    logger.info("Hi from stream‑only logger!")

Уровень задаётся переменной окружения ``LOG_LEVEL`` (по умолчанию INFO).
"""
from __future__ import annotations

import os
import sys

from loguru import logger

# ───────────────────────────── Env config ─────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ───────────────────────────── Sink setup ─────────────────────────────
logger.remove()

# Единственный sink — цветная консоль (stderr)
logger.add(
    sys.stderr,
    level=LOG_LEVEL,
    backtrace=True,
    diagnose=False,
    colorize=True,
    enqueue=True,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}:{line}</cyan> | "
        "<level>{message}</level>"
    ),
)

# ───────────────────────────── Noise filters ──────────────────────────
for noisy in ("urllib3", "asyncio", "sqlalchemy.engine"):
    logger.disable(noisy)

__all__: list[str] = ["logger"]
