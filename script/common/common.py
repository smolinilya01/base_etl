"""Common functions"""

import datetime as dt
import logging
import os
import sqlite3 as sql
import pandas as pd
import xlwings as xw
import win32com.client


class StartEndDateError(Exception):
    def __init__(self):
        print('start_date, end_date and date_col must were input all or not input all')


class XlFileError(Exception):
    def __init__(self):
        print('File excel not found in path')


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


def conn_bd_oemz():
    """
    Подключается к базе bd_oemz.bd3

    :return
        объект sqlite3.connection
    """
    conn = sql.connect(
        r"\\oemz-fs01.oemz.ru\Works$\Analytics\Database\bd_oemz.bd3",
        detect_types=sql.PARSE_COLNAMES | sql.PARSE_DECLTYPES
    )
    return conn


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


def insert_records_of_loads(cur1, name1):
    """
    Добавляет запись о добавлении данных в нужную таблицу в базе bd_oemz.bd3 !без commit!

    :arg
        name1 - наименование таблицы, куда добавилась запись, string like 'inputs'
        cur1 - объект sqlite3.connection.cursor
    """
    cur1.execute("""CREATE TABLE IF NOT EXISTS records_of_loads(
                    id INTEGER PRIMARY KEY,
                    date TIMESTAMP NOT NULL,
                    table_ TEXT NOT NULL)""")
    row = (dt.datetime.now(), name1)
    cur1.execute("""INSERT INTO records_of_loads(date,table_) VALUES (?,?)""", row)


def parse_date_path(path1):
    """
    Парсит полный путь и достает дату

    :arg
        path1 - путь типа '\\172.16.4.1\vp_164\report\10_10_2019.txt'

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


def symbols_for_query(name_table1, cur1):
    """
    Возвращает 2 списка

    :arg
        name_table1 - str like 'vp_164'
        cur1 - объект sqlite3.connection.cursor

    :returns
        cols_for_query - список столбцов в которые нужно добавить значения (все столбцы в таблице name_table1 кроме id)
        syms_for_query - список из необходиого кол-ва знаков ? для запроса
    """
    cols = return_name_cols(name_table1, cur1)
    cols_for_query = ','.join(cols)
    syms_for_query = ','.join(['?'] * len(cols))
    return cols_for_query, syms_for_query


def return_name_cols(table1, cur1):
    """
    Возвращает список наименований столбцов из таблицы table1 базы данных bd_oemz.bd3

    :arg
        table1 - str like 'vp_164'
        cur1 - объект sqlite3.connection.cursor

    :return
        tuple
    """
    cur1.execute(f"""pragma table_info({table1})""")
    names_col = cur1.fetchall()
    names_col = pd.DataFrame(names_col)
    names_col = names_col[1][names_col[1] != 'id']
    names_col = tuple(names_col.values)
    return names_col


def check_data_in_db(name_data1):
    """
    Проверяет, есть ли сегодня загруженные данные по необходимым данным. Возвращает bool
    Используется для определения, нужно ли производить формировние отчета

    :param name_data1: str наименование данных, которые надоп роверить
    :return: bool
    """
    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur_date = dt.datetime.now()
        cur_date1 = cur_date + dt.timedelta(days=1)
        cur.execute(f"""SELECT table_ FROM records_of_loads
                WHERE date BETWEEN '{cur_date.strftime("%Y-%m-%d")} %' and '{cur_date1.strftime("%Y-%m-%d")} %'""")
        availability = [i[0] for i in cur.fetchall()]
        return name_data1 in availability


def load_table_in_xlsheet(table1, sh_name1, path1):
    """
    Загружает таблицу на лист sh_name1 в файл эксель

    :param table1: таблица для добавления
    :param sh_name1: наименование листа для записи
    :param path1: путь к файлу ексель с макросом
    :return: execute добавляет данные на лист
    """
    app = xw.App(visible=False)
    wb = xw.Book(path1)
    ws = wb.sheets.add(name=sh_name1)
    ws.range('A1').options(index=False, header=False).value = table1
    wb.save()
    wb.close()
    app.kill()


def run_macro(path1, name_macros):
    """
    Выполняел макрос в файле ексель в модуле Module1 !!!!!!!!!!!!!!

    :param: path1: путь к файлу ексель с макросом
    :param: name_macros: наименование макроса
    :raise: XlFileError если файла не существует, то ошибка
    :return: execute
    """
    if os.path.exists(path1):
        excel_macro = win32com.client.DispatchEx("Excel.Application")
        excel_path = os.path.expanduser(path1)
        workbook = excel_macro.Workbooks.Open(Filename=excel_path, ReadOnly=1)
        query = os.path.basename(path1) + '!Module1.' + name_macros
        excel_macro.Application.Run(query)
        workbook.Save()
        excel_macro.Application.Quit()
        del excel_macro
    else:
        raise XlFileError


def load_data_from_db(table_name1, cols_name1, date_col=None, start_date=None, end_date=None):
    """
    Универсальная функция дял загрузки данных из базы для простого запроса

    :param table_name1: наименование таблицы в базе bd_oemz.bd3 в формате str
    :param cols_name1: список нужных столбцов в формате list
    :param date_col: наименование столбца с форматов Timestamp для отбора по нему в формате str
    :param start_date: дата начала выборки в формате dt.datetime
    :param end_date: дата конца выборки в формате dt.datetime (что бы выбрать включительно 2019.01.01,
                                нужно либо dt.datetime(2019,1,2) либо dt.datetime(2019,1,2,23,59,59)
    :raise: если date_col, start_date, end_date не все заполнены
    :return: pd.DataFrame
    """
    with conn_bd_oemz() as conn:
        cur = conn.cursor()

        if start_date is None and end_date is None and date_col is None:
            cur.execute(f"""SELECT {','.join(cols_name1)} FROM {table_name1}""")
        elif start_date is not None and end_date is not None and date_col is not None:
            start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""SELECT {','.join(cols_name1)} FROM {table_name1}
                            WHERE {date_col} BETWEEN '{start_date}' and '{end_date}'""")
        else:
            raise StartEndDateError

        data = pd.DataFrame(data=cur.fetchall(), columns=cols_name1)
        return data


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


def convert_xltime(table1, cols1):
    """
    Конвертирует колонки cols1 в таблице table1 из секунд в понятный для экселя формат

    :param table1: dp.DataFrame таблица с данными
    :param cols1: list список с наименованием колонок для преобразования
    :return: dp.DataFrame с преобразованными колонками cols
    """
    for i in cols1:
        table1[i] = table1[i] / (24 * 60 * 60)
    return table1


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
