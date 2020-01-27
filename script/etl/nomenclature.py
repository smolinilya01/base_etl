"""loading nomenclature in DB"""

import pandas as pd

from script.common.common import (conn_bd_oemz, insert_records_of_loads)


def load_nomenclature():
    """
    Загружает в базу данных bd_oemz.bd3 справочник номенклатур
    """
    with conn_bd_oemz() as conn:
        cur = conn.cursor()
        cur.execute(""" CREATE TABLE if not EXISTS nomenclature(
                        code INTEGER PRIMARY KEY,
                        name TEXT,
                        unite TEXT,
                        indicator TEXT)""")
        cur.execute(""" CREATE INDEX IF NOT EXISTS nomenclature_code
                        on nomenclature(code)""")
        cur.execute("""DELETE FROM nomenclature""")
        data_dict = pd.read_excel(
            r"\\oemz-fs01.oemz.ru\Works$\Analytics\Выгрузки из УПП\Справочник\list_nom.XLS"
        ).values
        cur.executemany(
            """INSERT INTO nomenclature (code,name,unite,indicator) VALUES (?,?,?,?)""",
            data_dict
        )
        insert_records_of_loads(cur, 'nomenclature')
        conn.commit()
