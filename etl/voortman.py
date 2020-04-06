"""ETL voortman data"""

from common.database import conn_pobeda
from common.common import date_range
from pandas import read_sql_query, DataFrame, read_csv
from datetime import date


NEED_MACHINE = 'Voortman V304'


def prepare_voortman_data() -> None:
    """Подготовливает данные по станку voortman в файлы"""
    with conn_pobeda() as conn:
        details_table(conn)  # создание voortman_1 и voortman_2

    general_table()


def details_table(conn: conn_pobeda) -> None:
    """Подготовка таблицы с деталями а так же

    :param: conn: соединение с базой победы
    """
    query = """SELECT * FROM VIEW_MadePosition"""
    data = read_sql_query(query, conn)

    all_machines = data[['number_of_task', 'name_device']].drop_duplicates()
    cards_table(conn=conn, merge_table=all_machines)  # создание voortman_1

    data = data.query(f"name_device == '{NEED_MACHINE}'")
    data['date_done'] = data['date_done'].\
        map(lambda x: date(year=x.year, month=x.month, day=x.day))
    data = data.sort_values(by='date_done', ascending=False)

    need_columns = [
        'date_done', 'number_of_task', 'card_number',
        'numbermarki', 'position', 'countpos',
        'massaonepos', 'perimeter', 'full_mass',
        'full_perimeter'
    ]
    data = data[need_columns]

    data.to_csv(
        r'.\common\files\voortman_2.csv',
        sep=";",
        encoding='ansi',
        index=False
    )


def cards_table(conn: conn_pobeda, merge_table: DataFrame) -> None:
    """Подготовка таблицы с картами раскроя

    :param: conn: соединение с базой победы
    :param: merge_table: таблица с номерами заданий и соответствующим станком
    """
    query = """SELECT * FROM VIEW_CuttingsCards"""
    data = read_sql_query(query, conn)

    data = data.merge(merge_table, on='number_of_task', how='left')
    data = data.query(f"name_device == '{NEED_MACHINE}'")
    del data['name_device']
    data['date_done'] = data['date_done'].\
        map(lambda x: date(year=x.year, month=x.month, day=x.day))
    data = data.sort_values(by='date_done', ascending=False)

    data['sheets_mass'] = data['sheets_amount'] * data['massa_of_sheet']
    data['full_time'] = data['sheets_amount'] * data['full_calc_time']
    data['sum_perimeter'] = data['sheets_amount'] * data['all_full_perimeter']

    data['full_calc_time'] = data['full_calc_time'].\
        map(lambda x: x / 24)  # екселевский формат времени = доля от дня
    data['full_time'] = data['full_time'].\
        map(lambda x: x / 24)  # екселевский формат времени = доля от дня

    data.to_csv(
        r'.\common\files\voortman_1.csv',
        sep=";",
        encoding='ansi',
        index=False
    )


def general_table() -> None:
    """Создает итоговую таблицу по дням с КПД и КИО"""
    cards = read_csv(
        r'.\common\files\voortman_1.csv',
        sep=";",
        encoding='ansi',
        parse_dates=['date_done']
    )

    sum_mass_days = cards.groupby(by=['date_done'])\
        ['massa_of_sheet'].\
        sum().\
        reset_index().\
        rename(columns={'massa_of_sheet': 'day_mass'})
    cards = cards.merge(sum_mass_days, how='left', on='date_done')

    cards['KIM'] = cards['coefficient_of_use'] * (cards['massa_of_sheet'] / cards['day_mass'])

    cards = cards.groupby(by=['date_done'])\
        ['KIM', 'sheets_amount', 'full_time'].\
        sum().\
        reset_index()
    cards['KPD'] = cards['full_time'] / (10.75 * 2 / 24)  # не по сменам, а по дням
    cards['KIO'] = cards['full_time'] / (12 * 2 / 24)  # не по сменам, а по дням

    dates = date_range(cards.loc[:, 'date_done'])\
        ['date'].\
        unique()
    table = DataFrame(data=dates, columns=['date_done']).\
        sort_values(by='date_done', ascending=False).\
        merge(cards, on='date_done', how='left').\
        fillna(value=0)

    need_columns = [
        'date_done', 'KPD', 'KIO',
        'sheets_amount', 'KIM', 'full_time'
    ]
    table = table[need_columns]
    table.to_csv(
        r'.\common\files\voortman_3.csv',
        sep=";",
        encoding='ansi',
        index=False
    )
