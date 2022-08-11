# forecast-revenue
# Описание

</details>
Сервис прогнозирования объема выручки по всем сетям поминутно на 1 день вперед. Сервис необходим для расчета потерь от инцидентов в дашборде по инцидентам. Сам алгоритм основан на анализе временного ряда по выборке правдоподобия. Подробное описание алгоритма https://cf.x5food.tech/pages/viewpage.action?pageId=55317615

Алгоритм на данный момент не учитывает ничего, кроме исторических данных по выручке.

# Требования
- python 3.8+

# Запуск приложения
Перед сборкой положить в каталог файл .env с параметрами подключения к бд postgres.

**Запуск в виртуальном окружении**

`python -m venv venv`

`source venv/bin/activate`

`pip install --upgrade pip`

`pip install -r requirements.txt`



`export $(xarg < .env) (for Linux) или export $(cat .env | xargs -L 1) (for Mac os)`

`python3 main.py`



### Запуск в докере:
```bash
cd forecast-revenue/
docker build --no-cache -t forecast_revenue_calc .
docker run -d --env-file .env --rm forecast_revenue_calc
```


Запуск сервиса  на продуктиве происходит один раз в сутки  через systemd по расписанию 01:00 Europe/Moscow времени.

Ежедневно происходит расчет только за предыдущий день!

**Пример файла .env**

SERVICE_NAME=revenue_forecast_calc

ENVIRONMENT=prod

LOG_LEVEL=INFO

CALC_MODE=all

OUT_TABLE_FORECAST=ml_common.forecast_revenue

DB_HOST=pg-db01.int.x5food.tech

DB_PORT=15544

DB_NAME=postgres

DB_USER=revenue_forecast_calc

DB_PASS=*******

STATSD_HOST=bioyino.x5food.tech

STATSD_PORT=8126


**Переменные окружения**


| Название | Описание | Пример                     |
| ------ | ------ |----------------------------|
| SERVICE_NAME | Название сервиса | revenue_forecast_calc      |
| ENVIRONMENT | Тип окружения | prod                       |
| LOG_LEVEL | Уровень логгирования | INFO                       |
| CALC_MODE | Тип расчета | all/day                    |
| OUT_TABLE_FORECAST | Таблица для записи результатов | ml_common.forecast_revenue |
| DB_HOST | Хост для бд хранилища | pg-db01.int.x5food.tech    |
| DB_PORT | Порт для бд хранилища | 15544                      |
| DB_NAME | Название для бд хранилища | postgres                   |
| DB_USER | Пользователь для хранилища | revenue_forecast_calc      |
| DB_PASS | Пароль для хранилища | *******                    |
