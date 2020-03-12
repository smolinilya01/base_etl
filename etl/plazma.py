"""Load data from plazma in DB"""

import datetime as dt
import pandas as pd
import os
import xlrd

from typing import Union
from common.common import to_datetime_in_list
from common.database import (conn_bd_oemz, symbols_for_query)
from common.error import MethodAccountingFileError


def load_plazma_tables(path1: str) -> None:
    """
    Создание таблицы и индекса, если их не сущещствовало прописано в need_plazma_files.

    :arg path1 - путь к папке с файлами по плазме (для сравнения с файлами из базы)
    """
    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        npf = need_plazma_files(path1)  # npf - need_plazma_files
        data = collect_plazma_data(npf.path.values, npf)  # данные для загрузки в таблицу plazma_cards

        npf = to_datetime_in_list(npf.values, 2)  # преобразование в numpy.array и dt_change в dt.datetime
        cur.executemany(
            """INSERT INTO plazma_files(path,base_name,dt_change) VALUES(?,?,?)""",
            npf
        )  # поместили список файлов в базу

        cur.execute("""CREATE TABLE IF NOT EXISTS plazma_cards(
        id INTEGER PRIMARY KEY,
        path TEXT, 
        dt_change TIMESTAMP,
        path_in TEXT,
        mass REAL,
        cut_amount REAL, 
        stub_amount REAL,
        cut_time REAL, 
        move_time REAL, 
        stub_time REAL,
        cut_length REAL, 
        move_length REAL,
        cut_square REAL, 
        move_square REAL, 
        stub_square REAL, 
        umc REAL,
        cut_weight REAL, 
        move_weight REAL, 
        stub_weight REAL,
        size_length INTEGER,
        size_width INTEGER,
        size_thickness INTEGER)""")
        cur.execute("""CREATE INDEX IF NOT EXISTS plazma_cards_dt on plazma_cards(dt_change)""")

        columns_for_query_1, symbols_for_query_1 = symbols_for_query('plazma_cards', cur)  # списки колонок и вопросиков для запроса
        query_1 = f"""INSERT INTO plazma_cards({columns_for_query_1}) VALUES ({symbols_for_query_1})"""
        cur.executemany(query_1, to_datetime_in_list(data.values, 1))
        insert_records_of_loads(cur, 'plazma')
        conn.commit()


def need_plazma_files(path1) -> pd.DataFrame:
    """
    Сначала пытаемся создать таблицу, потом пытаемся получить список файлов, загруженных в базу bd_oemz.bd3

    :arg path1 - путь к папке с файлами по плазме (для сравнения с файлами из базы)
    :return pd.DataFrame с данными о файлах, которые нужно загрузить с колонками 'path', 'base_name', 'dt_change'
    """
    exceptions_ = [
        r'W:\Plasma\REPORT\2019\ФЕВРАЛЬ 2019\ссз № 84040 карта № 7134                 10мм               12.02.2019.xls',
        r"W:\Plasma\REPORT\2020\Март 2020\8\по распоряжению зам нач цеха                           6мм                        07.03.2020.xls"
    ]  # файлы, которые не открываются или в неправильном или нечитаемом формате
    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS plazma_files(
        id INTEGER PRIMARY KEY,
        path TEXT,
        base_name text,
        dt_change TIMESTAMP)""")
        cur.execute("""CREATE INDEX IF NOT EXISTS plazma_files_dt on plazma_files(dt_change)""")
        conn.commit()

        cur.execute("""SELECT path, base_name, dt_change FROM plazma_files""")

        inbase_files = pd.DataFrame(data=cur.fetchall(), columns=['path', 'base_name', 'dt_change'])
        inbase_files['ind'] = (
                inbase_files.base_name +
                inbase_files.dt_change.map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f"))
        )                                              # индикатор, состоящий из base_name и dt_change

        need_files = get_plazma_files(path1)
        need_files['ind'] = (
                need_files.base_name +
                need_files.dt_change.map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f"))
        )                                                  # индикатор, состоящий из base_name и dt_change
        need_files = need_files[~need_files['path'].isin(exceptions_)]  # убираем файлы, которые не читаеются etc

        need_set = set(need_files.ind.values) - set(inbase_files.ind.values)  # need_set - множество с индикаторами, которые надо добавить
        need_files = need_files[['path', 'base_name', 'dt_change']][need_files.ind.isin(need_set)]
        need_files = need_files.sort_values(by=['dt_change'])
        return need_files


def get_plazma_files(path1) -> pd.DataFrame:
    """
    Возвращает таблицу с информацией по файлам PLAZMA

    :arg path1 - путь к папке с файлами по плазме

    :return pd.DataFrame (
    1 колонка - путь к файлу
    2 колонка - basename
    3 колонка - datetime изменения файла
    )
    """
    files = os.walk(path1)
    files = [r[0] + '\\' + i for r in files for i in r[2] if len(r[2]) > 0 if 'xls' in i]  # choice only xls files
    files = pd.DataFrame(data=files, columns=['path'])
    files['base_name'] = files['path'].map(lambda x: os.path.basename(x))
    files['dt_change'] = files['path'].map(date_accounting_file(method='change'))
    files = files.sort_values(by='dt_change')
    return files


def date_accounting_file(method: str = 'create'):
    """Выбираем какую дату учета файла использовать"""
    if method == 'create':
        return lambda x: dt.datetime.fromtimestamp(os.path.getctime(x))
    elif method == 'change':
        return lambda x: dt.datetime.fromtimestamp(os.path.getmtime(x))
    else:
        raise MethodAccountingFileError


def collect_plazma_data(pl_files1, table1) -> pd.DataFrame:
    """
    Функция собирает данные из файловпо плазме и формирует таблицу для загрузки в базу bd_oemz.bd3.
    Используются практически все параметры, указанные в файле (на возможное будущее)
    Столбцы с time измеряются в секундах

    :arg pl_files1 - список файлов like numpy.array
    :arg table1 - таблица pd.Dataframe из need_plazma_files
    :return pd.DataFrame с информацией из карты
    """
    coordinates = ((3, 2), (6, 2), (6, 7),
                   (14, 3), (16, 3),
                   (14, 5), (15, 5), (16, 5),
                   (14, 7), (15, 7),
                   (14, 11), (15, 11), (16, 11), (18, 11),
                   (14, 13), (15, 13), (16, 13))  # координаты нужных ячеек в формате (row, col)
    cols_name = ('path', 'dt_change',
                 'path_in', 'size', 'mass',
                 'cut_amount', 'stub_amount',
                 'cut_time', 'move_time', 'stub_time',
                 'cut_length', 'move_length',
                 'cut_square', 'move_square', 'stub_square', 'umc',
                 'cut_weight', 'move_weight', 'stub_weight')  # umc - use material coef

    data = []
    for f in pl_files1:  # собирает данные в цикле в список
        row = list()
        row.append(f)  # первое значение путь к файлу
        row.append(table1['dt_change'][table1.path == f].values[0])  # второе значение дата изменения

        book = xlrd.open_workbook(f, encoding_override='ansi', logfile=open(os.devnull, 'w'))
        sheet = book.sheet_by_index(0)
        for i in coordinates:  # заполняются значения из файла начиная с третьего места в row
            row.append(sheet.cell_value(*i))
        data.append(row)

    data = pd.DataFrame(data=data, columns=cols_name)
    for i in data.columns[4:]:  # преобразовывает значения в цифры
        data[i] = data[i].map(del_empty)
        data[i] = data[i].map(float)

    data['size'] = data['size'].map(get_sizes)
    data['size_length'], data['size_width'], data['size_thickness'] = \
        data['size'].map(lambda x: int(x[0])), data['size'].map(lambda x: int(x[1])), data['size'].map(lambda x: int(x[2]))
    del data['size']
    return data


def del_empty(x) -> Union[str, int]:
    """
    Удаляем лишние эелементы из цифр и заменяем '' на 0

    :arg x - элемент в Series
    """
    if type(x) is str:
        x = x.strip()
        x = x.replace(' ', '')
    if x == '':
        x = 0
    return x


def get_sizes(text_size) -> list:
    """
    Пребразует текстовое поле формата '6010mmx1500mmx4mm' в 3 величины: длина, ширина, толщина

    :arg text_size - текстовое поле формата '6010mmx1500mmx4mm'
    :return list(length, width, sickness)  - length, width, sickness in str
    """
    text_size = text_size.replace('x', '')
    text_size = text_size.split('mm')
    return text_size


def insert_records_of_loads(cur1, name1) -> None:
    """
    Добавляет запись о добавлении данных в нужную таблицу в базе bd_oemz.bd3 !без commit!

    :arg name1 - наименование таблицы, куда добавилась запись, string like 'inputs'
    :arg cur1 - объект sqlite3.connection.cursor
    """
    cur1.execute("""CREATE TABLE IF NOT EXISTS records_of_loads(
                    id INTEGER PRIMARY KEY,
                    date TIMESTAMP NOT NULL,
                    table_ TEXT NOT NULL)""")
    row = (dt.datetime.now(), name1)
    cur1.execute("""INSERT INTO records_of_loads(date,table_) VALUES (?,?)""", row)
