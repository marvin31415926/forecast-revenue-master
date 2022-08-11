import datetime
import logging
import pandas as pd
from pandas import DataFrame

from storage.postgres_db import PostgresqlDB

logger = logging.getLogger(__name__)


class CsvFile:
    """Обработчик по работе с Excel"""

    csvFileName = "revenue.csv"
    """Имя файла"""

    def get_data(self) -> DataFrame:
        """Функция получения данных из csv"""
        try:
            data = pd.read_csv(self.csvFileName)
        except Exception as err:
            logger.error(f"Error with get data from csv: {err}")
            raise
        return data


class Loader:
    # загрузка данных по выручке

    table_forecast: str
    # таблица для прогнозных данных
    storage: PostgresqlDB
    # Хранилище для сохранения данных
    csvFileName = "revenue.csv"

    # Имя файла для сохранения данных

    def __init__(self, db_storage: PostgresqlDB, out_table_forecast: str):
        self.storage = db_storage
        self.table_forecast = out_table_forecast

    def get_aggregate_all_dates(self) -> DataFrame:

        # получение всех данных
        sql = """with calendar_minute as (
                select   make_timestamp(extract(year from a)::INTEGER, extract (month from a)::INTEGER, 
                extract (day from a)::INTEGER, tm.time, tmm.time, 0) as date_d,
                    hos.host as host
                  from (
                    SELECT  CAST('2021-05-01' AS DATE) + (n || ' day')::interval as a
                    FROM    generate_series(0, 1500) n
                     ) tbl,
                    (select * from generate_series(0 ,23) time) tm,
                    (select * from generate_series(0 ,59) time) tmm,
                    (select 'cross' as host
                    union all
                    select 'five' as host
                    union all
                    select 'karusel' as host
                    union all
                    select 'okolo' as host) hos
                   where a <= current_date
                ),
                ord_src as (
                select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                    'Cross' as host,
                    count(*) as orders,
                    sum(o.price_total) as revenue_rur
                    from rt_cross.orders o
                where o.status_id = 9
                    and o.created_at::date >= '2021-05-01'
                group by DATE_TRUNC('minute', o.created_at)
                union all
                select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                    'five' as host,
                    count(*) as orders,
                    sum(o.price_total) as revenue_rur
                    from rt_five.orders o
                where o.status_id = 9
                    and o.created_at::date >= '2021-05-01'
                group by DATE_TRUNC('minute', o.created_at)
                union all
                select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                    'karusel' as host,
                    count(*) as orders,
                    sum(o.price_total) as revenue_rur
                    from rt_karusel.orders o
                where o.status_id = 9
                    and o.created_at::date >= '2021-05-01'
                group by DATE_TRUNC('minute', o.created_at)
                union all
                select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                    'okolo' as host,
                    count(*) as orders,
                    sum(o.price_total) as revenue_rur
                    from rt_okolo.orders o
                where o.status = 'done'
                    and o.created_at::date >= '2021-05-01'
                group by DATE_TRUNC('minute', o.created_at))
                select c.date_d as order_date_min, c.host, COALESCE(o.orders, 0) as orders, 
                COALESCE(o.revenue_rur, 0) as revenue_rur from calendar_minute c
                left join ord_src o
                on c.date_d=o.order_date_min and o.host = c.host"""
        try:
            cursor = self.storage.connection.cursor()
            cursor.execute(sql)
            data = pd.DataFrame(cursor.fetchall(),
                                columns=["order_date_min", "orders", "revenue_rur"])
            cursor.close()
        except Exception as err:
            logger.error(f"Error in get all data from db with err: {err}")
            raise
        return data

    def save_all_data(self) -> None:
        # Функция сохранения всех данных из бд
        try:
            df = self.get_aggregate_all_dates()
            df.to_csv(self.csvFileName)
        except Exception as err:
            logger.error(f"Error in saving with err: {err}")
            raise

    def get_history_two_week(self, dt: datetime) -> DataFrame:
        # функция возвращает данные по выручке за 2 недели
        end_date = dt + datetime.timedelta(30)
        sql = """with calendar_minute as (
                        select   make_timestamp(extract(year from a)::INTEGER, extract (month from a)::INTEGER, 
                        extract (day from a)::INTEGER, tm.time, tmm.time, 0) as date_d,
                            hos.host as host
                          from (
                            SELECT  CAST('{0}' AS DATE) + (n || ' day')::interval as a
                            FROM    generate_series(0, 1500) n
                             ) tbl,
                            (select * from generate_series(0 ,23) time) tm,
                            (select * from generate_series(0 ,59) time) tmm,
                            (select 'cross' as host
                            union all
                            select 'five' as host
                            union all
                            select 'karusel' as host
                            union all
                            select 'okolo' as host) hos
                           where a <= current_date
                        ),
                        ord_src as (
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'cross' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_cross.orders o
                        where o.status_id = 9
                            and o.created_at::date >= '{0}'
                            and o.created_at::date <= '{1}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'five' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_five.orders o
                        where o.status_id = 9
                            and o.created_at::date >= '{0}'
                            and o.created_at::date <= '{1}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'karusel' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_karusel.orders o
                        where o.status_id = 9
                            and o.created_at::date >= '{0}'
                            and o.created_at::date <= '{1}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'okolo' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_okolo.orders o
                        where o.status = 'done'
                            and o.created_at::date >= '{0}'
                            and o.created_at::date <= '{1}'
                        group by DATE_TRUNC('minute', o.created_at))
                        select c.date_d as order_date_min, 
                               c.host as host,
                               COALESCE(o.orders, 0) as orders, 
                               COALESCE(o.revenue_rur, 0) as revenue_rur
                        from calendar_minute c
                        left join ord_src o
                        on c.date_d=o.order_date_min and o.host = c.host
                        where c.date_d::DATE <= '{1}' and c.date_d::DATE >= '{0}'
                        order by c.date_d""".format(str(dt), str(end_date))
        try:
            cursor = self.storage.connection.cursor()
            cursor.execute(sql)
            data = pd.DataFrame(cursor.fetchall(),
                                columns=["order_date_min", "host", "orders", "revenue_rur"])
            cursor.close()
        except Exception as err:
            logger.error(f"Error in get all data from db with err: {err}")
            raise
        return data

    def get_history_one_day(self, dt: datetime) -> DataFrame:
        # функция возвращает данные по выручке за предыдущий день
        sql = """with calendar_minute as (
                        select   make_timestamp(extract(year from a)::INTEGER, extract (month from a)::INTEGER, 
                        extract (day from a)::INTEGER, tm.time, tmm.time, 0) as date_d,
                            hos.host as host
                          from (
                            SELECT  CAST('{0}' AS DATE) + (n || ' day')::interval as a
                            FROM    generate_series(0, 1500) n
                             ) tbl,
                            (select * from generate_series(0 ,23) time) tm,
                            (select * from generate_series(0 ,59) time) tmm,
                            (select 'cross' as host
                            union all
                            select 'five' as host
                            union all
                            select 'karusel' as host
                            union all
                            select 'okolo' as host) hos
                           where a <= current_date
                        ),
                        ord_src as (
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'cross' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_cross.orders o
                        where o.status_id = 9
                            and o.created_at::date = '{0}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'five' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_five.orders o
                        where o.status_id = 9
                            and o.created_at::date = '{0}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'karusel' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_karusel.orders o
                        where o.status_id = 9
                            and o.created_at::date = '{0}'
                        group by DATE_TRUNC('minute', o.created_at)
                        union all
                        select  DATE_TRUNC('minute', o.created_at) as order_date_min,
                            'okolo' as host,
                            count(*) as orders,
                            sum(o.price_total) as revenue_rur
                            from rt_okolo.orders o
                        where o.status = 'done'
                            and o.created_at::date = '{0}'
                        group by DATE_TRUNC('minute', o.created_at))
                        select c.date_d as order_date_min, 
                               c.host as host,
                               COALESCE(o.orders, 0) as orders, 
                               COALESCE(o.revenue_rur, 0) as revenue_rur 
                        from calendar_minute c
                        left join ord_src o
                        on c.date_d=o.order_date_min and o.host = c.host
                        where c.date_d::DATE = '{0}'
                        order by c.date_d""".format(str(dt))
        try:
            cursor = self.storage.connection.cursor()
            cursor.execute(sql)
            data = pd.DataFrame(cursor.fetchall(),
                                columns=["order_date_min", "host", "orders", "revenue_rur"])
            cursor.close()
        except Exception as err:
            logger.error(f"Error in get all data from db with err: {err}")
            raise
        return data

    def save_history_one_week(self, dt: datetime) -> int:
        # функция сохранения исторических данных за один день
        try:
            df = self.get_history_two_week(dt)
            df['revenue_rur'] = df['revenue_rur'].astype(float)
            sum_orders_df = df.groupby('order_date_min').sum()[['orders']]
            sum_revenue_df = df.groupby('order_date_min').sum()[['revenue_rur']]
            sum_df = pd.concat([sum_orders_df, sum_revenue_df], axis=1)
            sum_df['host'] = 'all'
            sum_df['order_date_min'] = sum_df.index
            df = pd.concat([df, sum_df], ignore_index=True)
            cnt = len(df)
            df.to_csv(self.csvFileName, index=False)
        except Exception as err:
            cnt = 0
            logger.error(f"Error in saving with err: {err}")
        return cnt

    def save_history_one_day(self, dt: datetime) -> int:
        # функция сохранения исторических данных за один день
        try:
            # считываем старые данные
            data = pd.read_csv(self.csvFileName)
            # удаляем первую дату, чтобы не хранить лишние данные
            delete_date = datetime.datetime.strptime(data['order_date_min'][0], '%Y-%m-%d %H:%M:%S').date()
            data = data.loc[pd.to_datetime(data['order_date_min']).dt.date != delete_date]
            # считываем новые данные
            df = self.get_history_one_day(dt)
            df['revenue_rur'] = df['revenue_rur'].astype(float)
            sum_orders_df = df.groupby('order_date_min').sum()[['orders']]
            sum_revenue_df = df.groupby('order_date_min').sum()[['revenue_rur']]
            sum_df = pd.concat([sum_orders_df, sum_revenue_df], axis=1)
            sum_df['host'] = 'all'
            sum_df['order_date_min'] = sum_df.index
            df = pd.concat([df, sum_df], ignore_index=True)
            # df.append(sum_df, ignore_index=True)
            data = pd.concat([data, df], ignore_index=True)
            cnt = len(data)
            data.to_csv(self.csvFileName, index=False)
            # , mode='a', header=False)
        except Exception as err:
            logger.error(f"Error in saving with err: {err}")
            cnt = 0
        return cnt

    def check_table(self, first_day: str) -> str:
        sql = f"select data from {self.table_forecast} where data::DATE='{first_day}'"
        try:
            cursor = self.storage.connection.cursor()
            cursor.execute(sql)
            if cursor.rowcount > 1:
                res = 'one day'
            else:
                res = 'all'
            cursor.close()
        except Exception as err:
            logger.error(f"Error in get data from db with err: {err} with sql: {sql}")
            raise
        return res

    def save_forecast(self, data: DataFrame) -> None:
        # функция сохранения исторических данных за один день
        try:
            self.storage.write_data(data, self.table_forecast)
        except Exception as err:
            logger.error(f"Error in saving in db with err: {err}")
            raise
