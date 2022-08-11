from statsd import StatsClient
import datetime
import time
import logging
import pandas as pd
from core.loader import Loader
from core.calculator import Calculator
from storage.postgres_db import PostgresqlDB
from config.settings import Settings, settings


def fetch_config() -> Settings:
    # получаем конфиг
    return settings


def init_logger(cfg: Settings) -> logging.Logger:
    # инициализируем логгер
    logging.basicConfig(level=cfg.LOG_LEVEL.value)
    handler, formatter = logging.StreamHandler(), logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    return logger


def connect_database(cfg: Settings):
    try:
        db_engine = PostgresqlDB(host=cfg.DB_HOST,
                                 port=cfg.DB_PORT,
                                 dbname=cfg.DB_NAME,
                                 username=cfg.DB_USER,
                                 password=cfg.DB_PASS)
        db_engine.connect()
    except Exception as err:
        logger.error(f"unable to connect to storage with err {err}")
        raise
    return db_engine


if __name__ == "__main__":
    # минимальное количество дней для расчета прогноза на один день
    MINIMAL_COUNT_OF_DAYS = 15
    # FIRST_DAY = datetime.date(2022, 1, 1)
    FIRST_DAY = datetime.date(2022, 7, 1)

    time_begin = int(time.time())
    config = fetch_config()
    logger = init_logger(cfg=config)
    logger.info(f"Application starting at {datetime.datetime.now()}")

    root_metric_path = 'services.dwh.' + config.SERVICE_NAME + '.' + config.ENVIRONMENT
    logger.debug(f"root_metric_path {root_metric_path}")

    statsd = StatsClient(host=config.STATSD_HOST,
                         port=config.STATSD_PORT,
                         prefix=root_metric_path
                         )
    statsd.incr('start')

    try:

        storage = connect_database(cfg=config)
        loader = Loader(db_storage=storage, out_table_forecast=config.OUT_TABLE_FORECAST)
    except Exception as err:
        logger.critical(f"unable to get parameters with err {err}")
        raise

    calc_mode = 'all'  # loader.check_table(str(FIRST_DAY + datetime.timedelta(1)))
    if calc_mode == 'one day':
        try:
            # 1. loading and saving history data
            # для расчета одного дня нам нужны данные за 2 недели
            logger.info("loading history last two weeks")
            dt = (datetime.datetime.now() - datetime.timedelta(days=MINIMAL_COUNT_OF_DAYS)).date()
            cnt_history = loader.save_history_one_week(dt)
            statsd.gauge('code.history', cnt_history)
        except Exception as err:
            statsd.gauge('success', 0)
            statsd.gauge('code.history', 0)
            logger.error(f"unable load last one week with err {err}")
            raise

        try:
            # 2. Calculate forecast
            logger.info("Calculate last day")
            calc = Calculator()
            forecast, five_forecast, cross_forecast, rmse, cnt_forecast = calc.calculate('day')
            statsd.gauge('code.forecast', cnt_forecast)
            statsd.gauge('code.forecast_error', rmse / cnt_forecast)
        except Exception as err:
            statsd.gauge('success', 0)
            statsd.gauge('code.forecast', 0)
            logger.error(f"unable calculate last day with err {err}")
            raise
    else:
        try:
            # 1. loading and saving history data
            logger.info("loading history first one week")
            cnt_history = loader.save_history_one_week(FIRST_DAY)
        except Exception as err:
            statsd.gauge('success', 0)
            logger.error(f"unable load with err {err}")
            raise

        try:
            # 2. Calculate forecast
            logger.info("Calculate first one week")
            calc = Calculator()
            forecast, five_forecast, cross_forecast, rmse_forecast, cnt_forecast = calc.calculate('all')
        except Exception as err:
            statsd.gauge('success', 0)
            statsd.gauge('code.forecast', 0)
            logger.error(f"unable calculate data with err {err}")
            raise

        dt = FIRST_DAY + datetime.timedelta(MINIMAL_COUNT_OF_DAYS)
        while dt <= datetime.date.today():
            try:
                # 1. loading and saving history data
                logger.info("loading history one day")
                cnt_history += loader.save_history_one_day(dt)
            except Exception as err:
                statsd.gauge('success', 0)
                logger.error(f"unable load one day {dt} with err {err}")
                raise

            try:
                # 2. Calculate forecast
                logger.info(f"Calculate one day {dt}")
                calc = Calculator()
                all_forecast, five_result_forecast, cross_result_forecast, rmse, cnt = calc.calculate('one_day')
                cnt_forecast += cnt
                rmse_forecast += rmse
                forecast = pd.concat([forecast, all_forecast], ignore_index=True)
                five_forecast = pd.concat([five_result_forecast, five_forecast], ignore_index=True)
                cross_forecast = pd.concat([cross_result_forecast, cross_forecast], ignore_index=True)
            except Exception as err:
                statsd.gauge('success', 0)
                logger.error(f"unable calculate data with err {err}")
                raise
            dt = dt + datetime.timedelta(1)
        statsd.gauge('code.history', cnt_history)
        statsd.gauge('code.forecast', cnt_forecast)
        statsd.gauge('code.forecast_error', rmse_forecast / cnt_forecast)
        logger.info(f"RMSE {rmse_forecast / cnt_forecast}")

    try:
        # 3. Save the result
        forecast['host'] = 'all'
        five_forecast['host'] = 'five'
        cross_forecast['host'] = 'cross'
        forecast = pd.concat([forecast, five_forecast, cross_forecast])
        loader.save_forecast(forecast)
    except Exception as err:
        statsd.gauge('success', 0)
        logger.critical(f"unable to save forecast with err {err}")
        raise

    statsd.gauge('success', 1)
    time_end = int(time.time())
    length = time_begin - time_end
    statsd.gauge('code.duration', length)
    statsd.incr('stop')
    logger.info(f"Application completed at {datetime.datetime.now()} in {time_end - time_begin} sec")
