"""Build report voortman machine"""

import shutil
import datetime as dt

from common.excel import run_macro
from pandas import read_csv

PATH_FILE = r'.\common\files\voortman.xlsm'


def voortman_report() -> None:
    """формирует отчет по voortman через макрос ексель"""
    run_macro(PATH_FILE, 'voortman')

    table = read_csv(
        r'.\common\files\voortman_1.csv',
        sep=";",
        encoding='ansi',
        parse_dates=['date_done']
    )

    min_date = table['date_done'].min().strftime("%y%m%d")
    max_date = table['date_done'].max().strftime("%y%m%d")
    cur_date = dt.datetime.now().date().strftime("%y%m%d")
    name_copy_f = f"{cur_date}_Отчёт_по_Плазме-Voortman_за_период_{min_date}-{max_date}.xlsm"
    copy_path = r"W:\1.1. Отчеты по производству\1.1.4 Отчет по Плазме-Voortman" + "\\" + name_copy_f
    shutil.copy(PATH_FILE, copy_path)
