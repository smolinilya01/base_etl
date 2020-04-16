"""Reports for factory visits"""

import shutil
import datetime as dt

from common.excel import run_macro


def visit_report() -> None:
    """Подготовливает отчет по проходной"""
    path = r'.\common\files\visit.xlsm'
    run_macro(path1=path, name_macros='visit')

    cur_date = dt.datetime.now().date().strftime("%y%m%d")
    name_copy_f = f"{cur_date}_Отчёт_по_выходу_рабочих.xlsm"
    copy_path = r"W:\1.1. Отчеты по производству\1.1.7 Отчет по выходу рабочих" + \
                "\\" + name_copy_f
    shutil.copy(path, copy_path)

