import random
import pandas as pd
import numpy as np
from pandas import DataFrame
from scipy import stats

from core.loader import CsvFile


class Calculator:
    # класс самого калькулятора
    # длина прогноза
    L: int = 1440
    # Параметры модели по выборке максимального подобия
    # длина выборки максимального подобия
    M: int = 1440
    # шаг поиска
    step: int = 1440
    # данные по выручке
    data: pd.DataFrame = pd.DataFrame()
    # итоговый общий прогноз
    forecast_data: pd.DataFrame = pd.DataFrame()
    # итоговый прогноз по пятерочки
    five_forecast_data: pd.DataFrame = pd.DataFrame()
    # итоговый прогноз по перекрестку
    cross_forecast_data: pd.DataFrame = pd.DataFrame()
    # флаг для помощи расчету
    flag: bool = True

    def __init__(self):
        self.data = self.read_data()

    # считывание данных для расчета
    @staticmethod
    def read_data() -> DataFrame:
        # для теста
        csv_data = CsvFile()
        data = csv_data.get_data()
        data['order_date_min'] = pd.to_datetime(data['order_date_min'])
        return data

    # функиця нахождения максимального правдоподобия
    def find_most_likeness(self, index: int, hist_new_data: DataFrame, current_data: DataFrame) -> int:
        k = 1
        likeness = np.zeros((index, 2))
        currentWeekDay = hist_new_data['order_date_min'][index + 1].weekday()
        if index > 3 * self.M + 2 * self.L:
            while index >= self.M + 1:
                # 3 * self.M + 2 * self.L - self.step:
                hist_old_data = current_data[index - self.M - 1: index - 1]
                if ((currentWeekDay == 0 and hist_old_data['order_date_min'][index - 2].weekday() > 4) or
                    (currentWeekDay > 4 and hist_old_data['order_date_min'][index - 2].weekday() < 5)):
                    index = index - self.step  # Откат по времени назад на шаг Step
                    continue
                likeness[k, 0] = index
                # Проверка на то, нулевые ли векторы
                if len(hist_old_data.ne(0).idxmax()) > 0 and len(hist_new_data.ne(0).idxmax()) > 0:
                    likeness[k, 1] = abs(stats.pearsonr(hist_old_data['revenue_rur'], hist_new_data['revenue_rur'])[0])
                else:
                    likeness[k, 1] = 0
                k = k + 1
                index = index - self.step  # Откат по времени назад на шаг Step

            # 3) Определяем максимум подобия
            index = int(likeness[np.argmax(abs(likeness[:, 1])), 0])
            self.flag = True
        else:
            index = index + self.step
            self.flag = False
        return index

    # решение матричного уравнения
    @staticmethod
    def solve_matrix(hist_old_data: DataFrame, hist_new_data: DataFrame) -> np.ndarray:
        # 6) Делаем аппроксимацию HistNew при помощи HistOld по методу наименьших
        # квадратов (в данном случае решение находится через обратную матрицу)
        x = hist_old_data['revenue_rur'].to_numpy()
        # Добавляем столбец с единичным ветором I
        x0 = np.ones(len(hist_old_data))
        x = np.vstack((x, x0))
        y = hist_new_data['revenue_rur'].to_numpy()
        xn = x.dot(np.transpose(x))
        yn = x.dot(y)
        inv_x = np.linalg.inv(xn)
        # Исковые коэффициенты регрессии A (то, что находится по формуле 3.4)
        a = inv_x.dot(np.transpose(yn))
        return a

    # корректировка прогноза (зашумление)
    @staticmethod
    def correct_forecast(forecast: np.ndarray, a: np.ndarray):
        i = 0
        mean = np.mean(forecast)
        correct_forecast = forecast.copy()
        for f in forecast:
            if f == a[1]:
                forecast[i] = 0
                correct_forecast[i] = 0
            else:
                forecast[i] = forecast[i] * 1.1
                correct_forecast[i] = (forecast[i] + np.random.normal(0, 1) * mean / 100) * 1.1
            i += 1
        return forecast, correct_forecast

    # сам расчет
    def calculate(self, calc_mode: str):
        # для регулировки полного/неполного расчета
        if calc_mode == 'all':
            koef = 1
        else:
            koef = 30
        cnt = 0
        rmse = 0
        for curr_host in ['five', 'cross', 'all']:
            current_data = self.data[self.data['host'] == curr_host].reset_index(drop=True)
            for i in range(koef * self.L + 1, len(current_data), self.M):
                # Момент прогноза - это i

                # для понедельника и субботы особый алгоритм
                if (current_data['order_date_min'][i].weekday() == 0 or
                    current_data['order_date_min'][i].weekday() == 5) and \
                        i - 6 * self.L  - self.step > 0:
                    hist_new_data = current_data[i - 6 * self.L - self.M - 1: i - 6 * self.L - 1]
                    # новый индекс для дальнейшего расчета
                    index = i - 6 * self.L - self.step
                else:
                    hist_new_data = current_data[i - self.M - 1: i - 1]
                    # новый индекс для дальнейшего расчета
                    index = i - self.step

                index = self.find_most_likeness(index, hist_new_data, current_data)

                # 4) Определяем массив Старой истории (HistOld)
                hist_old_data = current_data[index - self.M - 1: index - 1]

                if self.flag:
                    # 5) Определяем Базовую историю (BaseHist)
                    if self.flag:
                        hist_base_data = current_data[index - 1: index + self.L - 1]
                    else:
                        hist_base_data = hist_old_data

                    a = self.solve_matrix(hist_old_data, hist_new_data)

                    # 7) Прогнозирование
                    x = hist_base_data['revenue_rur'].to_numpy()
                    x0 = np.ones(len(hist_base_data))
                    x = np.vstack((x, x0))
                    # Вектор, содержащий прогноз
                    _forecast = np.transpose(x).dot(a)
                    # скорректированный прогноз
                    forecast, correct_forecast = self.correct_forecast(_forecast, a)
                    rmse += np.median(abs(forecast - current_data['revenue_rur'][i - self.L - 1: i - 1]) / (
                            current_data['revenue_rur'][i - self.L - 1: i - 1] + 1))
                    cnt += 1

                    df = pd.DataFrame({'data': current_data['order_date_min'][i - 1: i + self.L - 1].values,
                                       'forecast': forecast,
                                       'correct_forecast': correct_forecast},
                                      columns=['data', 'forecast', 'correct_forecast'])

                else:
                    df = pd.DataFrame({'data': current_data['order_date_min'][i - 1: i + self.L - 1].values,
                                       'forecast': current_data['revenue_rur'][i - 1: i + self.L - 1].values,
                                       'correct_forecast': current_data['revenue_rur'][i - 1: i + self.L - 1].values},
                                      columns=['data', 'forecast', 'correct_forecast'])
                if curr_host == 'five':
                    self.five_forecast_data = pd.concat([self.five_forecast_data, df])
                elif curr_host == 'cross':
                    self.cross_forecast_data = pd.concat([self.cross_forecast_data, df])
                else:
                    self.forecast_data = pd.concat([self.forecast_data, df])

        return self.forecast_data, self.five_forecast_data, self.cross_forecast_data, rmse, cnt
