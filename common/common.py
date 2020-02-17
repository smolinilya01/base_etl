"""Common functions"""

import datetime as dt
import logging
import os
import pandas as pd


def check_func(foo, arg=None):
    """
    Пытается исполнить функцию foo, если не удаются, то пишет в "..\LOG_LOAD_DB.log" сообщение о падении

    :arg
        foo - исполняемая функция
        arg - должен быть формата (x,) или (x1,x2)
    """
    if arg is None:
        try:
            foo()
        except:
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                filename=r"..\LOG_LOAD_DB.log"
            )
            logging.info(f"{foo.__name__} was down")
    else:
        try:
            foo(*arg)
        except:
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                filename=r"..\LOG_LOAD_DB.log"
            )
            logging.info(f"{foo.__name__} was down")


def parse_date_point(x1):
    """
    Парсит даты типа 01012019 и преобразует в datetime.datetime

    :arg
        x1 - string like '01012019'
    """
    date = x1.replace('.', '')
    return dt.datetime(year=int(date[4:]), month=int(date[2:4]), day=int(date[:2]))


def to_datetime_in_list(table1, n1):
    """
    Функция принимает numpy.array и на месте n1 преобразовывает pandas.Timestamp в datetime

    :arg
        table1 - таблица в виде numpy.array (как список из списков)
        n1 - место в итерируемом списке
        """
    for i in table1:
        i[n1] = dt.datetime.fromtimestamp(dt.datetime.timestamp(i[n1]))
    return table1


def parse_date_path(path1):
    """
    Парсит полный путь и достает дату

    :arg
        path1 - путь типа "W:\VP_164\report\17_02_2020.txt"

    :return
        dt.datetime
    """
    date = os.path.basename(path1).replace('.txt', '')  # отделяем basename и удаляем .txt
    date = date.split('_')  #
    date = dt.datetime(year=int(date[2]), month=int(date[1]), day=int(date[0]))
    return date


def zero_time_dt(dt1):
    """
    Зануляет время в объекте dt.datetime (2019, 1, 1, 12, 5, 5) => (2019, 1, 1, 0, 0, 0)

    :arg
        dt1 -  объект dt.datetime

    :return
        dt.datetime
    """
    dt1 = dt.datetime(year=dt1.year, month=dt1.month, day=dt1.day)
    return dt1


def true_date(dt1):
    """
    Определяет дату со сдвигом, т.е. если время до 8-00 то этот день относиться к предыдущему дню

    :param dt1: объект типа dt.datetime
    :return: dt.date
    """
    border = dt.time(hour=8, minute=0, second=0)
    if dt1.time() < border:
        return (dt1 - dt.timedelta(days=1)).date()
    else:
        return dt1.date()


def def_smena(dt1):
    """
    Определяет смену по datetime: 1 смена с 8-00 до 19-59-59, иначе 2 смена

    :param dt1: объект типа dt.datetime
    :return: номер смена типа '1 смена'
    """
    low_border = dt.time(hour=8, minute=0, second=0)
    high_border = dt.time(hour=20, minute=0, second=0)
    if (dt1.time() >= low_border) and (dt1.time() < high_border):
        return 1
    else:
        return 2


def date_range(series1):
    """
    Принимает pd.Series преобразовывает в pd.DataFrame, если пропущены даты, то заполняет их и определяет верхнуюю
    границу каксегодняшнее число, однако сегодняшнее число невходит в итоговый tuple (так как утром делается за
    предыдущие дни)

    :param series1: pd.Series с dates
    :return: pd.DataFrame для мержа с остальными данными
    """
    min_date = series1.min()
    max_date = dt.datetime.now().date()
    tuple_dt = []
    i = min_date
    while i < max_date:
        tuple_dt.append(i)
        i += dt.timedelta(days=1)

    table = pd.DataFrame(data=((d, s) for d in tuple_dt for s in (1, 2)), columns=['date', 'smena'])
    return table
