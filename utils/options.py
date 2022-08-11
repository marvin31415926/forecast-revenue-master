from enum import Enum
import logging
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING


class LoggingLevels(str, Enum):
    """Перечисление доступных уровней логгирования"""

    CRITICAL: str = logging.getLevelName(CRITICAL)
    ERROR: str = logging.getLevelName(ERROR)
    WARNING: str = logging.getLevelName(WARNING)
    INFO: str = logging.getLevelName(INFO)
    DEBUG: str = logging.getLevelName(DEBUG)


class Environments(str, Enum):
    """Перечисление доступных сред"""
    DEVELOPMENT: str = "dev"
    """Среда локальной разработки приложения"""
    PRODUCTION: str = "prod"
    """Боевая среда приложения"""
