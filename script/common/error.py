"""Errors"""


class StartEndDateError(Exception):
    def __init__(self):
        print('start_date, end_date and date_col must were input all or not input all')


class XlFileError(Exception):
    def __init__(self):
        print('File excel not found in path')
