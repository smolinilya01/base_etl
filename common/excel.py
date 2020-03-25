"""Excel functions"""

import os
import xlwings as xw
import win32com.client

from common.error import XlFileError


class ContManagExcel(xw.App):
    """xw.App с поведением контекстного менеджера"""
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # print(traceback)
        self.kill()


def load_table_in_xlsheet(table1, sh_name1, path1):
    """
    Загружает таблицу на лист sh_name1 в файл эксель

    :param table1: таблица для добавления
    :param sh_name1: наименование листа для записи
    :param path1: путь к файлу ексель с макросом
    :return: execute добавляет данные на лист
    """
    path1 = os.path.abspath(path1)
    with ContManagExcel(visible=False) as app:
        wb = xw.Book(path1)
        try:
            wb.sheets[sh_name1].delete()
        except:
            pass

        ws = wb.sheets.add(name=sh_name1)
        ws.range('A1').options(index=False, header=False).value = table1
        wb.save()
        wb.close()


def run_macro(path1, name_macros):
    """
    Выполняел макрос в файле ексель в модуле Module1 !!!!!!!!!!!!!!

    :param: path1: путь к файлу ексель с макросом
    :param: name_macros: наименование макроса
    :raise: XlFileError если файла не существует, то ошибка
    :return: execute
    """
    if os.path.exists(path1):
        path1 = os.path.abspath(path1)
        excel_path = os.path.expanduser(path1)
        try:
            excel_macro = win32com.client.DispatchEx("Excel.Application")
            workbook = excel_macro.Workbooks.Open(Filename=excel_path, ReadOnly=1)
            query = os.path.basename(path1) + '!Module1.' + name_macros
            excel_macro.Application.Run(query)
            workbook.Save()
            excel_macro.Application.Quit()
            del excel_macro
        except:
            excel_macro.Application.Quit()
            del excel_macro
            raise BaseException(f'Ошибка в работе макроса в файле {excel_path}')
    else:
        raise XlFileError


def convert_xltime(table1, cols1):
    """
    Конвертирует колонки cols1 в таблице table1 из секунд в понятный для экселя формат

    :param table1: dp.DataFrame таблица с данными
    :param cols1: list список с наименованием колонок для преобразования
    :return: dp.DataFrame с преобразованными колонками cols
    """
    for i in cols1:
        table1[i] = table1[i] / (24 * 60 * 60)
    return table1
