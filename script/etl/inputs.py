"""loading inputs in DB"""

import datetime as dt
import glob
import pandas as pd
import os

from script.common.common import (
    conn_bd_oemz, parse_date_point, to_datetime_in_list
)


def load_inputs():
    """
    Загружаем в базу данных bd_oemz.bd3 список.
    Обязательно помним, что pandas.Timestamp не помещается в sqlite3, нужно переводить в другой формат
    """
    path_last_file = pick_last_inputs()
    table = pd.read_excel(
        path_last_file,
        usecols=[
            'Номер4', 'Дата', 'НомерВходящегоДокумента', 'Склад',
            'Количество', 'Цена', 'Код_УПП', 'ИНН_Контрагента'
        ],
        parse_dates=['Дата'], dayfirst=True)
    table = table.rename(columns={
        'Номер4': 'input_id', 'Дата': 'date', 'НомерВходящегоДокумента': 'inputdoc_id',
        'Склад': 'stock', 'Количество': 'amount', 'Цена': 'price_without_vat',
        'Код_УПП': 'nom_code', 'ИНН_Контрагента': 'client_inn'
    })
    table = table.dropna(subset=['client_inn', 'nom_code'])
    table.client_inn = table.client_inn.map(int)
    table.price_without_vat = table.price_without_vat.replace({None: 0})
    table = to_datetime_in_list(table.values, 1)

    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur.execute(""" CREATE TABLE IF NOT EXISTS inputs(
                        id INTEGER PRIMARY KEY,
                        input_id INTEGER,
                        date TIMESTAMP,
                        inputdoc_id TEXT,
                        stock TEXT,
                        amount REAL,
                        price_without_vat REAL,
                        nom_code INTEGER,
                        client_inn INTEGER,
                        FOREIGN KEY(nom_code) REFERENCES nomenclature(code),
                        FOREIGN KEY(client_inn) REFERENCES clients(inn))""")
        cur.execute(""" CREATE INDEX IF NOT EXISTS inputs_nom_code on inputs (nom_code)""")
        cur.execute(""" CREATE INDEX IF NOT EXISTS inputs_date on inputs (date)""")
        cur.execute("""DELETE FROM  inputs""")
        cur.executemany(""" INSERT INTO inputs(input_id,date,inputdoc_id,stock,amount,
                            price_without_vat,nom_code,client_inn)
                            VALUES (?,?,?,?,?,?,?,?)""", table)
        insert_records_of_loads(cur, 'inputs')
        conn.commit()


def pick_last_inputs():
    """
    Выбирает последний созданный файл с поступлениями в папке выгрузок.
    Определение даты происзодит по цифрам в названии файла.
    """
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Выгрузки из УПП\Цены и индекс\*.XLS*"
    list_files = glob.glob(path)
    list_files = [(i, os.path.basename(i).split('_')[1][:10]) for i in list_files]
    table_path_date = pd.DataFrame(list_files, columns=['path', 'date'])
    table_path_date['date'] = table_path_date['date'].map(parse_date_point)
    return table_path_date['path'][
        table_path_date['date'] == table_path_date['date'].max()
    ].values[0]


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
