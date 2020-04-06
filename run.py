"""
Execution script with windows planner
Импортирует etl и auto_report и выполняет нужные функции
"""

import datetime as dt
import os

from common.common import check_func
from common.database import check_data_in_db
from etl.nomenclature import load_nomenclature
from etl.inputs import load_inputs
from etl.clients import load_clients
from etl.vp import load_vp
from etl.plazma import load_plazma_tables
from etl.voortman import prepare_voortman_data
from report.plazma import prep_plaz_xlfile
from report.vp import prep_vp_xlfile
from report.voortman import voortman_report


if __name__ == '__main__':
    os.chdir(r'C:\LOG_1\base_etl')  # need for correct execute in windows planner
    CUR_YEAR = dt.datetime.now().year
    VP_164 = 'VP_164'
    VPX_94 = 'VPX_94'
    PATH_PLAZMA = r'W:\Plasma\REPORT\{0}'.format(CUR_YEAR)

    """Блок с ETL можно сделать в мультипроцессе ТОЧНО!"""
    check_func(load_nomenclature)
    check_func(load_inputs)
    check_func(load_clients)
    check_func(load_vp, (VP_164,))
    check_func(load_vp, (VPX_94,))
    check_func(load_plazma_tables, (PATH_PLAZMA,))
    check_func(prepare_voortman_data)

    """Блок с REP можно сделать в мультипроцессе ВОЗМОЖНО(нужно проверить работу с ексель)!"""
    if check_data_in_db('plazma'):
        check_func(prep_plaz_xlfile)
    if check_data_in_db('vpx_94'):
        check_func(voortman_report)
    if check_data_in_db('vp_164'):
        check_func(prep_vp_xlfile, ('vp_164',))
    if check_data_in_db('vpx_94'):
        check_func(prep_vp_xlfile, ('vpx_94',))
