"""
Execution script with windows planner
Импортирует etl и auto_report и выполняет нужные функции
"""

import datetime as dt

from script.common.common import (check_func, check_data_in_db)
from script.etl.nomenclature import load_nomenclature
from script.etl.inputs import load_inputs
from script.etl.clients import load_clients
from script.etl.vp import load_vp
from script.etl.plazma import load_plazma_tables
from script.report.plazma import prep_plaz_xlfile
from script.report.vp import prep_vp_xlfile


if __name__ == '__main__':
    cur_year = dt.datetime.now().year

    """Блок с ETL можно сделать в мультипроцессе ТОЧНО!"""
    check_func(load_nomenclature)
    check_func(load_inputs)
    check_func(load_clients)
    check_func(load_vp, ('vpx_94',))
    check_func(load_vp, ('vp_164',))
    check_func(load_plazma_tables, (r'\\172.16.4.1\plasma\REPORT\{0}'.format(cur_year),))

    """Блок с REP можно сделать в мультипроцессе ВОЗМОЖНО(нужно проверить работу с ексель)!"""
    if check_data_in_db('plazma'):
        check_func(prep_plaz_xlfile)
    if check_data_in_db('vp_164'):
        check_func(prep_vp_xlfile, ('vp_164',))
    if check_data_in_db('vpx_94'):
        check_func(prep_vp_xlfile, ('vpx_94',))
