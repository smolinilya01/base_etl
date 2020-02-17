"""loading data from VP in DB"""

import datetime as dt
import glob
import pandas as pd
import re

from common.common import (
    parse_date_path, zero_time_dt, to_datetime_in_list
)
from common.database import (
    conn_bd_oemz, insert_records_of_loads, symbols_for_query
)


def load_vp(name_table1: str) -> None:
    """
    Загружает данные по станку в таблицу name_table1 базы bd_oemz.bd3

    :arg name_table1 - наименование таблицы либо 'vp_164', либо 'vpx_94'
    """
    name_table1 = name_table1.lower()  # поменялись пути к файлам, теперь они с заклавными буквами
    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {name_table1}(
        id INTEGER PRIMARY KEY,
        c_0 TIMESTAMP,          
        c_1 TEXT,           c_2 TEXT,               
        c_3 TEXT,           c_4 TEXT,               
        c_5 REAL,           c_7 INTEGER,
        c_8 INTEGER,        c_9 REAL,
        c_10 INTEGER,       c_11 INTEGER,
        c_12 INTEGER,       c_13 INTEGER,
        c_14 INTEGER,       c_15 INTEGER,
        c_16 INTEGER,       c_17 INTEGER,
        c_18 INTEGER,       c_19 INTEGER,
        c_20 INTEGER,       c_21 INTEGER,
        c_22 INTEGER,       c_23 INTEGER,
        c_24 INTEGER,       c_25 INTEGER,
        c_26 TEXT,          c_27 TEXT,
        c_28 TEXT,          c_29 TEXT,
        c_30 TEXT,          c_31 TEXT,
        c_32 TEXT,          c_33 TEXT,
        c_34 TEXT,          c_35 TEXT,
        c_36 TEXT,          c_37 TEXT,
        c_38 TEXT,          c_39 REAL,
        c_40 REAL,          c_41 REAL,
        c_42 TEXT,          c_43 TEXT,
        c_44 REAL,          c_45 REAL,
        c_46 REAL,          c_47 TEXT)""")  # создание таблицы, если ее не существует
        cur.execute(f""" CREATE INDEX IF NOT EXISTS {name_table1}_date
                        on {name_table1}(c_0)""")

        last_date = last_date_from_vp_db(cur, name_table1)  # получение даты и времени последней записи в таблице
        table_files = load_list_files_vp(name_table1)  # список всех путей к файлам и даты файлов
        filter_date = (table_files.date >= zero_time_dt(last_date))   # фильтр таблицы, где выбираются файлы с датой >= последней дате из базы
        table_files = table_files[filter_date]

        done_table = table_from_txt(table_files.path.values)  # таблица из txt файлов по нужным файлам (датам)
        done_table = reshape_table_from_txt(done_table, last_date)  # таблица из txt файл

        columns_for_query_1, symbols_for_query_1 = symbols_for_query(name_table1, cur)  # списки колонок и вопросиков для запроса
        query_1 = f"""INSERT INTO {name_table1}({columns_for_query_1}) VALUES ({symbols_for_query_1})"""
        cur.executemany(query_1, done_table)
        insert_records_of_loads(cur, name_table1)
        conn.commit()


def last_date_from_vp_db(cur1, name_table1) -> dt.datetime:
    """
    Загружает последнюю дату (timestamp) записи из таблицы из базы bd_oemz.bd3
    Если вдруг нет данных в таблице и вернулся None, то значение last_date = dt.datetime(year=2000, month=1, day=1),
    это позволит сделать выборку со всеми записями, ничего не отбрасывая.

    :arg cur1 - объект sqlite3.connection.cursor
    :arg table_vp1 - str like 'vp_164' таблица, из которой нужно получить последнюю дату
    """
    cur1.execute(f"""SELECT MAX(c_0) FROM {name_table1}""")
    last_date = cur1.fetchall()[0][0]
    if last_date is None:
        last_date = dt.datetime(year=2000, month=1, day=1)
    else:
        last_date = dt.datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
    return last_date


def table_from_txt(paths1) -> pd.DataFrame:
    """
    Собирает в цикле таблицу из текстовых файлов по станкам vp, файлы уже взяты с нужной датой

    :arg paths1 - список путей к нужным файлам (которые удовлетворяют по дате из filter_date),
    буду добавлять туда table_files.path.values
    """
    done_table = pd.DataFrame()
    for i in paths1:
        cur_table = pd.read_csv(i, sep=';', encoding='ansi', header=None)
        done_table = pd.concat([done_table, cur_table])
    return done_table


def reshape_table_from_txt(table1, last_date1) -> pd.DataFrame:
    """
    Преобразование таблицы из функции table_from_txt() для загрузки в базу
        - объединяет столбцы 0 и 6 в один столбец 0 с форматом datetime (автоматом в timestamp переводится)
        - 6 столбец удаляется
        - выбираются данные после последней записи в таблице
        - преобразовывает из pandas.DataFrame в list
        - преобразовывает 0 столбец из pandas в datetime.datetime

    :arg table1 - таблица из table_from_txt в формате pandas.DataFrame
    :arg last_date1 - дата и время последней записи из таблицы
    """
    table1[0], table1[6] = table1[0].map(lambda x: x.strip()), table1[6].map(lambda x: x.strip())  # удаляем пробелы по бокам
    table1[0], table1[6] = table1[0].map(lambda x: re.findall(r'\d', x)), table1[6].map(lambda x: re.findall(r'\d', x))  # позвращаем список только с цифрами
    table1[0] = table1[0] + table1[6]
    del table1[6]
    table1[0] = table1[0].map(lambda x: ''.join(x))  # соединяем цифры в стиле "01122018010947"
    table1[0] = table1[0].map(lambda x: dt.datetime(
        year=int(x[4:8]), month=int(x[2:4]), day=int(x[0:2]),
        hour=int(x[8:10]), minute=int(x[10:12]), second=int(x[12:])
    ))
    table1 = table1[table1[0] > last_date1]
    table1 = table1.sort_values(by=0)
    table1 = to_datetime_in_list(table1.values, 0)
    return table1


def load_list_files_vp(name_table1) -> pd.DataFrame:
    """
    Создает таблицу файлов линий вп из папки.
    1 столбец путь к файлуб, 2 столбец дата файла.
    С помощью столбца с датами можно можно справнивать последнюю дату записи в базе bd_oemz.bd3

    :arg name_table1 - наименование таблицы либо 'vp_164', либо 'vpx_94'
    """
    path = r"W:\{0}\report\*.txt".format(name_table1)
    files = glob.glob(path)
    files = pd.DataFrame(data=files, columns=['path'])
    files['date'] = files.path.map(parse_date_path)
    files = files.sort_values(by=['date'])
    return files
