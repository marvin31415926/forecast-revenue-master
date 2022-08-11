from pydantic import BaseSettings
from utils.options import Environments
from utils.options import LoggingLevels


import os
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

class Settings(BaseSettings):
    # Основное
    SERVICE_NAME: str
    """Название сервиса"""
    ENVIRONMENT: Environments
    """Среда для запуска приложения"""
    LOG_LEVEL: LoggingLevels
    """Уровень логгирования"""

    # Настройки подключения к хранилищу
    DB_HOST: str
    """IP-адрес сервера на котором находится база данных"""
    DB_PORT: int
    """Порт сервера на котором находится база данных"""
    DB_NAME: str
    """Название базы данных"""
    DB_USER: str
    """Имя пользователя в базе данных"""
    DB_PASS: str
    """Пароль пользователя в базе данных"""

    # Настройки для сохранения данных
    OUT_TABLE_FORECAST: str
    """Таблица для прогнозных данных"""

    # Настройки для мониторинга
    STATSD_HOST: str
    """Хост для statsd"""
    STATSD_PORT = 8126
    """Порт для statsd"""


class Config:
    case_sensitive = True
    arbitrary_types_allowed = True
    env_file = '.env'


settings = Settings(_env_file = '.env')