"""ETL visits data"""
# import os
# os.chdir(r'W:\Analytics\Илья\!repositories\base_etl')
import datetime as dt

from pypyodbc import connect
from pandas import (
    read_sql_query, read_excel, DataFrame, Series
)
from typing import Union

# последний день - это опзавчера, что бы вчерашние данные правильно учесть
TRUE_LAST_DATE = dt.datetime.now() - dt.timedelta(days=2)
LAST_DATE = dt.datetime.now() - dt.timedelta(days=1)
FIRST_DATE = dt.datetime.now() - dt.timedelta(days=32)


def prepare_visit_data() -> None:
    """Подготовка данных и формирование csv файлов для отчета"""
    connection = connect(
        "Driver={SQL Server};"
        "Server=OEMZ-BOLID.oemz.ru;"
        "Database=OEMZ;"
        "uid=sa;pwd=123456"
    )
    table_1, table_2 = prepare_visit_tables(conn=connection)


def prepare_visit_tables(conn: connect) -> Union[DataFrame, DataFrame]:
    """Подготовка 2 таблиц для отчета

    :param: conn: соединение с базой СКУД
    """
    # %Y-%d-%m день месяц наоборот
    main_query = f"""
    select 
        TimeVal, HozOrgan, Mode
    from 
        OEMZ.dbo.pLogData 
    where 
        DoorIndex = 1 and 
        Event = 32 and
        TimeVal BETWEEN '{FIRST_DATE.strftime('%Y-%d-%m 00:00:00')}' 
            and '{LAST_DATE.strftime('%Y-%d-%m 00:00:00')}'
    order BY
        TimeVal
    """
    data = read_sql_query(main_query, conn)

    # формирование справочника по сотрудникам
    person_query = """select ID, Name, FirstName, MidName from OEMZ.dbo.pList"""
    person_list = read_sql_query(person_query, conn)
    person_list['person'] = (
        person_list['name'] + ' ' + person_list['firstname'] + ' ' + person_list['midname']
    ).map(lambda x: x.strip())

    dict_employee = prepare_dict_employee()
    dict_employee = dict_employee.merge(person_list, on='person', how='left')

    # продолжение подготовки данных
    data = data.merge(
        dict_employee,
        left_on='hozorgan',
        right_on='id'
    )
    del data['hozorgan']


def prepare_dict_employee() -> DataFrame:
    """Подготавливает таблицу """
    path = r'.\common\files\employee.xlsx'
    data = read_excel(path)
    data = data.rename(columns={
        'ФизическоеЛицо': 'person',
        'Должность': 'post',
        'Подразделение': 'division',
        'ГрафикРаботы': 'graph'
    })

    # 1 - пятидневка, 2 - сменный график (2/2 и тд)
    data['graph'] = data['graph'].map(lambda x: 1 if 'График № 1' in x else 2)
    return data

