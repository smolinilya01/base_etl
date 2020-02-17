"""Preparation vp report"""

import datetime as dt
import shutil

from common.excel import (
    load_table_in_xlsheet, run_macro, convert_xltime,
)
from common.common import (
    zero_time_dt, true_date, def_smena, date_range
)
from common.database import load_data_from_db


def prep_vp_xlfile(vpname1: str) -> None:
    """
    Подгаталивает файл эксель (загружает нужные данные на технические листы), выполняет макрос в файле и копирует
    готовый файл в папку с отчетами по плазме.

    :arg vpname1: str наименование vp линии в формате 'vp-94'
    """
    folder_path = {'vpx_94': '1.1.2 Отчёт по VPX 94', 'vp_164': '1.1.1 Отчёт по VP 164'}
    detail_table = prep_detail_vptable(vpname1)
    gen_table = prep_gen_vptable(detail_table)
    kio_table, kpd_table = prep_plot_vpdata(gen_table)

    path_file = r".\common\files\{0}.xlsm".format(vpname1)
    load_table_in_xlsheet(detail_table, '1', path_file)
    load_table_in_xlsheet(gen_table, '2', path_file)
    load_table_in_xlsheet(kio_table, '3', path_file)
    load_table_in_xlsheet(kpd_table, '4', path_file)

    run_macro(path_file, 'vp_format')

    min_date = gen_table.date.min().strftime("%y%m%d")
    max_date = gen_table.date.max().strftime("%y%m%d")
    cur_date = dt.datetime.now().date().strftime("%y%m%d")
    name_copy_f = f"{cur_date}_Отчёт_по_{vpname1}_за_период_{min_date}-{max_date}.xlsm"
    copy_path = r"W:\1.1. Отчеты по производству\{0}".format(folder_path[vpname1]) + "\\" + name_copy_f
    shutil.copy(path_file, copy_path)


def prep_detail_vptable(vpname1: str):
    """
    Подготовка таблицы с детализированной информацией по vp линии

    :param vpname1: str наименование таблицы в виде 'vp_94'
    :return: pd.DataFrame
    """
    cols_name = ['c_0', 'c_4', 'c_3', 'c_9', 'c_7', 'c_17']
    cur_date = dt.datetime.now().date()
    start_date = zero_time_dt(cur_date) - dt.timedelta(days=46)
    end_date = zero_time_dt(cur_date) + dt.timedelta(days=1)

    table = load_data_from_db(vpname1, cols_name, date_col='c_0', start_date=start_date, end_date=end_date)
    table = table.rename(columns={
        'c_0': 'datetime', 'c_4': 'nomenclature', 'c_3': 'marka',
        'c_9': 'mass', 'c_7': 'dur_oper', 'c_17': 'ind_plet'
    })

    table['date'] = table['datetime'].map(true_date)
    table['smena'] = table['datetime'].map(def_smena)
    table['start_oper'] = table['datetime']
    table['end_oper'] = (table['datetime'] + table['dur_oper'].map(lambda x: dt.timedelta(seconds=x)))
    table['ind_plet'] = table['ind_plet'].replace({2: 1, 1: 0})
    table = convert_xltime(table, ['dur_oper'])
    table = table[['date', 'smena', 'nomenclature', 'marka', 'mass', 'start_oper', 'end_oper', 'dur_oper', 'ind_plet']]
    table = table[~(table['date'] == table['date'].min())]  # самая ранняя (маленькая) дата убирается, т.к. при рассчете true_date датасо временем до 8-00 переноситься на предыдущий день
    table = table.sort_values(by=['date', 'smena'], ascending=False)
    return table


def prep_gen_vptable(table1):
    """
    Преобразовывает таблицу из prep_detail_vptable в виде сводной таблицы для файла ексель

    :param table1: pd.DataFrame таблица из prep_detail_vptable
    :return: pd.DataFrame
    """
    cols_sum_table = ['date', 'smena', 'mass', 'dur_oper', 'ind_plet']
    time_for_plet = 73 / (24*60*60)  # время в экселевском формате
    c_kpd = ((10.75 * 60 * 60) / (24 * 60 * 60))  # делитель для расчета кпд оператора (10.75 часов - это 10-45)
    c_kio = ((12 * 60 * 60) / (24 * 60 * 60))  # делитель для расчета кио

    sum_table = table1[cols_sum_table].groupby(by=['date', 'smena']).sum().reset_index()
    sum_table['dur_oper'] = sum_table['dur_oper'] + (sum_table['ind_plet'] * time_for_plet)  # ко времени выполнения операций прибавляется время на плеть (time_for_plet)
    sum_table['kio'] = sum_table['dur_oper'] / c_kio
    sum_table['kpd'] = (sum_table['dur_oper'] * 1.21) / c_kpd
    sum_table = sum_table[['date', 'smena', 'kio', 'kpd', 'dur_oper', 'mass']]

    done_table = date_range(sum_table.date)  # для добавления нулей, если смена пропущена
    gen_table = done_table.merge(sum_table, how='outer', on=['date', 'smena']).replace({None: 0})
    gen_table = gen_table.sort_values(by=['date', 'smena'], ascending=False)  # сортировка от большего к меньшему
    return gen_table


def prep_plot_vpdata(table1):
    """
    Из таблицы prep_gen_vptable делатет 2 таблицы для графиков в файле ексель
    1) КИО по датам и сменам
    2) КПД по датам и сменам

    :param table1: pd.DataFrame таблица из prep_gen_vptable
    :return: pd.DataFrame 2 таблицы
    """
    kio_table = table1[['date', 'smena', 'kio']].set_index(['date', 'smena'])
    kio_table = kio_table.unstack('smena').reset_index()

    kpd_table = table1[['date', 'smena', 'kpd']].set_index(['date', 'smena'])
    kpd_table = kpd_table.unstack('smena').reset_index()
    return kio_table, kpd_table
