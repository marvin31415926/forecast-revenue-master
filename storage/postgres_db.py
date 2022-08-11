# модуль работы с хранилищем
from pydantic import BaseModel, Extra
import psycopg2
import psycopg2.extras
import pandas as pd
from pandas import DataFrame
import logging

logger = logging.getLogger(__name__)


class PostgresqlDB(BaseModel):
    """Обработчик по работе с базой данных Postgresql"""

    class Config:
        extra = Extra.allow
        arbitrary_types_allowed = True

    host: str
    """Хост для подключения к базе дданных"""
    port: int
    """Порт для подключения к базе дданных"""
    username: str
    """Имя пользователя зарегистрированного в базе данных"""
    password: str
    """Пароль пользователя зарегистрированного в базе данных"""
    dbname: str
    """Название базы данных Postgresql"""

    @property
    def connection(self) -> psycopg2.connect:
        if self.__db_connection is None:
            self.connect()
        return self.__db_connection

    def connect(self) -> None:
        try:
            logger.debug(f"start connection to postgresql to db <{self.dbname}>")
            self.__db_connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                port=self.port,
                host=self.host)
            logger.debug("database connection to postgresql to db <{self.dbname}> established")
        except Exception as err:
            logger.error(f"unable to connect to postgresql: {err}")
            raise ()

    def get_data(self, query) -> DataFrame:
        """Функция получения данных из бд"""
        try:
            df = pd.read_sql(query, self.connect)
        except Exception as err:
            logger.error(f"Error with get data from db: {err}")
            raise()
        return df

    def execute(self, query) -> bool:
        """Функция выполнения sql запроса в бд"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            return True
        except Exception as err:
            logger.error(f"Error in executing sql query: {err} with sql {query}")
            return False

    def write_data(self, df, table) -> bool:
        """Функция записи данных в бд"""
        try:
            df.to_csv('result.csv')

            # df_columns = list(df)
            # columns = ",".join(df_columns)
            # insert_statement = f"INSERT INTO {table} ({columns}) values %s;"
            # cursor = self.connection.cursor()
            # psycopg2.extras.execute_values(cursor, insert_statement, df.values)
            # self.connection.commit()
            # self.connection.close()
            return True
        except Exception as err:
            logger.error(f"Error in writing data to db in table {table} with error {err}")
            return False
