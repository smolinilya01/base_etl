"""Preparation plazma report"""

import datetime as dt
import os
import re
import os.path
import shutil

from script.common.common import (
    load_table_in_xlsheet, run_macro, zero_time_dt, load_data_from_db,
    true_date, def_smena, convert_xltime, date_range
)


def prep_plaz_xlfile():
    """
    Подгаталивает файл эксель (загружает нужные данные на технические листы), выполняет макрос в файле и копирует
    готовый файл в папку с отчетами по плазме.

    :return: execute
    """
    detail_table = prep_detail_plazdata()
    gen_table = prep_gen_plazdata(detail_table)
    plot_table = prep_plot_plazdata(gen_table)

    path_file = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\Задание 11-1\plazma.xlsm"
    load_table_in_xlsheet(detail_table, '1', path_file)
    load_table_in_xlsheet(gen_table, '2', path_file)
    load_table_in_xlsheet(plot_table, '3', path_file)

    run_macro(path_file, 'plazma_format')

    min_date = gen_table.date.min().strftime("%y%m%d")
    max_date = gen_table.date.max().strftime("%y%m%d")
    cur_date = dt.datetime.now().date().strftime("%y%m%d")
    name_copy_f = f"{cur_date}_Отчёт_по_Плазме_за_период_{min_date}-{max_date}.xlsm"
    copy_path = r"\\172.16.4.1\aup\1.Отчеты\1.1. Отчёты по производству\1.1.3 Отчёт по Плазме" + "\\" + name_copy_f
    shutil.copy(path_file, copy_path)


def prep_detail_plazdata():
    """
    Подгатавливает детализированную таблицу данных по ПЛАЗМЕ.
    Период данных определяется автоматически как от тек.дата - 30 дней до тек.дата.
    Форматы времени подготовлены под  excel (секунды / (24*60*60)

    :return: pd.DataFrame для загрузки на лист с детальными данными
    """
    cols_names = [
        'path', 'dt_change', 'path_in', 'mass', 'cut_amount', 'stub_amount', 'cut_time',
        'move_time', 'stub_time', 'cut_length', 'move_length', 'umc', 'size_thickness'
    ]                                                                 # колонки для sql query
    cur_date = dt.datetime.now()  # ткущая дата для расчета периода
    c_st = 0.18  # coefficient setup time (коэффициент подготовительно-заключительного времени
    cols_with_time = [
        'cut_time', 'move_time', 'stub_time', 'machine_time', 'setup_time', 'all_time'
    ]  # колонки с временными данными

    start_date = zero_time_dt(cur_date) - dt.timedelta(days=46)
    end_date = zero_time_dt(cur_date) + dt.timedelta(days=1)

    table = load_data_from_db('plazma_cards', cols_names,
                              date_col='dt_change', start_date=start_date, end_date=end_date)
    # начало преобразований в таблице +++++++++++++++++++++++++
    table['date'] = table['dt_change'].map(true_date)
    table['smena'] = table['dt_change'].map(def_smena)
    table['n_task'] = table['path_in'].map(num_task)
    table['n_card'] = table['path_in'].map(num_card)
    table['machine_time'] = table['cut_time'] + table['move_time'] + table['stub_time']
    table['setup_time'] = table['machine_time'] * c_st
    table['all_time'] = table['machine_time'] + table['setup_time']
    table = convert_xltime(table, cols_with_time)  # преобразование времени к экселевскому формату в нужных колонках (секунды / (24*60*60))
    table['operator'] = table['path'].map(num_oper)
    table = table[['date', 'smena', 'n_task', 'n_card', 'size_thickness', 'umc', 'mass', 'cut_amount', 'stub_amount',
                   'cut_length', 'move_length', 'cut_time', 'move_time', 'stub_time', 'machine_time', 'setup_time',
                   'all_time', 'operator']]  # правильный порядок столбцов
    table = table.sort_values(by=['date', 'smena'], ascending=False)  # сортировка от большего к меньшему
    table = table[~(table['date'] == table['date'].min())]  # самая ранняя (маленькая) дата убирается, т.к. при рассчете true_date датасо временем до 8-00 переноситься на предыдущий день
    return table


def prep_gen_plazdata(table1):
    """
    Преобразует table1 в таблице сгруппированную по дням и сменам с нужными коэфициентами и др

    :param table1: pd.DataFrame таблица из фу-ии prepare_plazma_detail
    :return: pd.DataFrame для загрузки на лист с общими данными (по дням и сманам)
    """
    c_kpd = ((10.75 * 60 * 60) / (24 * 60 * 60))  # делитель для расчета кпд оператора (10.75 часов - это 10-45)
    c_kio = ((12 * 60 * 60) / (24 * 60 * 60))  # делитель для расчета кио

    cols_fst = ['date', 'smena', 'all_time', 'machine_time', 'weight_umc']  # cols for sum_table
    mass_table = table1[['date', 'smena', 'mass']].groupby(by=['date', 'smena']).sum().reset_index()
    table1 = table1.merge(mass_table[['date', 'smena', 'mass']], on=['date', 'smena'])
    table1['weight_umc'] = (table1['mass_x'] / table1['mass_y']) * table1['umc']

    gen_table = table1[cols_fst].groupby(by=['date', 'smena']).sum().reset_index()
    gen_table['all_time'] = gen_table['all_time'] / c_kpd  # кпд оператора
    gen_table['kio'] = gen_table['machine_time'] / c_kio  # кио

    count_table = table1[['date', 'smena', 'n_task']].groupby(by=['date', 'smena']).count().reset_index()
    gen_table = gen_table.merge(count_table, on=['date', 'smena'])
    gen_table = gen_table[['date', 'smena', 'all_time', 'kio', 'n_task', 'weight_umc', 'machine_time']]

    done_table = date_range(gen_table.date)  # добавляет нули, если смена пропущена
    gen_table = done_table.merge(gen_table, how='outer', on=['date', 'smena']).replace({None: 0})
    gen_table = gen_table.sort_values(by=['date', 'smena'], ascending=False)  # сортировка от большего к меньшему
    return gen_table


def prep_plot_plazdata(table1):
    """
    Преобразует таблицу table1 в таблицу для применения рисовки графика в эксель

    :param table1: pd.DataFrame таблица из фу-ии prepare_plazma_general
    :return: pd.DataFrame для загрузки на лист с для создания графика
    """
    table1 = table1[['date', 'smena', 'kio']].set_index(['date', 'smena'])
    table1 = table1.unstack('smena').reset_index()
    table1 = table1.sort_values(by=['date'], ascending=False)
    return table1


def num_task(path1):
    """
    Парсит путь к файлу и ворачивает номер задания

    :param path1: str полный путь к файлу из ячейки самого файла
    :return: int номер задания
    """
    n_task = os.path.basename(path1)
    n_task = re.search(r"ССЗ\s*№\s*(\d+)", n_task)
    if n_task is None:
        return 0
    else:
        return int(n_task.group(1))


def num_card(path1):
    """
    Парсит путь к файлу и ворачивает номер карты

    :param path1: str полный путь к файлу из ячейки самого файла
    :return: int номер карты
    """
    n_card = os.path.basename(path1)
    n_card = re.search(r"карта\s*№\s*(\d+)", n_card)
    if n_card is None:
        return 0
    else:
        return int(n_card.group(1))


def num_oper(path1):
    """
    Парсит путь к файлу и ворачивает номер оператора

    :param path1: str полный путь к файлу
    :return: int номер оператора
    """
    n_oper = re.search(r'\\(\d)\\', path1)
    if n_oper is None:
        return None
    else:
        return int(n_oper.group(1))
