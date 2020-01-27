"""Working with database"""

import datetime as dt
import sqlite3 as sql
import pandas as pd

from script.common.error import StartEndDateError


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
