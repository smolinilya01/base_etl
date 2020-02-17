"""loading clients in DB"""

import pandas as pd

from common.database import (conn_bd_oemz, insert_records_of_loads)
from etl.inputs import pick_last_inputs


def load_clients():
    """Загружает в базу данных bd_oemz.bd3 список клиентов и их ИНН из таблицы поступлений"""
    path_last_file = pick_last_inputs()
    table = pd.read_excel(path_last_file, usecols=['ИНН_Контрагента', 'Контрагент'])
    table = table.rename(columns={'ИНН_Контрагента': 'inn', 'Контрагент': 'client'})
    table = table.drop_duplicates('inn').dropna()
    table.inn = table.inn.map(int)

    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur.execute(""" CREATE TABLE IF NOT EXISTS clients(
                        inn INTEGER PRIMARY KEY,
                        name TEXT NOT NULL)""")
        cur.execute("""DELETE FROM  clients""")
        cur.execute(""" CREATE INDEX IF NOT EXISTS clients_inn on clients(inn)""")
        cur.executemany("""INSERT INTO clients (name,inn) VALUES (?,?)""", table.values)
        insert_records_of_loads(cur, 'clients')
        conn.commit()
