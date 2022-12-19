from typing import List

from config import DB_URL                       # параметры подключения к БД из модуля конфигурации config.py
from .repository import sql_api                 # подключаем API для работы с БД
from .repository.connectorfactory import SQLStoreConnectorFactory

"""
    В данном модуле реализуются бизнес-логика обработки клиентских запросов.
    Здесь также могут применяться SQL-методы, представленные в модуле repository.sql_api
"""

# Структура основного навигационнго меню (<nav>) веб-приложения,
# оформленное в виде объекта dict

# navmenu = [
#     {
#         'name': 'HOME',
#         'addr': '/'
#     },
#     {
#         'name': 'ABOUT',
#         'addr': '#'
#     },
#     {
#         'name': 'CONTACT US',
#         'addr': '/contact'
#     },
# ]


def get_source_files_list() -> List[tuple]:
    result = []
    """ Получаем список обработанных файлов """
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)  # инициализируем соединение
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result.append(sql_api.select_all_from_source_files_neighbourhoods(db_connector))  # получаем список всех обработанных файлов
    result.append(sql_api.select_all_from_source_files_parks(db_connector))  # получаем список всех обработанных файлов
    result.append(sql_api.select_all_from_source_files_crime_rates(db_connector))  # получаем список всех обработанных файлов
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_processed_data_neighbourhoods(source_file: int, limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_all_from_neighbourhoods(db_connector)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_processed_points_neighbourhoods() -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_all_from_points_neighbourhoods(db_connector)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_processed_crime_rates(limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_rows_from_processed_data(db_connector, limit)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_processed_crime_rates(limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_rows_from_processed_data(db_connector, limit)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_low_crime_rates(limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_low_from_crime_rates(db_connector,limit)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_high_crime_rates(limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_high_from_crime_rates(db_connector,limit)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result

def get_middle_crime_rates(limit: int = None) -> List[tuple]:
    """ Получаем обработанные данные из табл neighbourhoods"""
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_middle_from_crime_rates(db_connector,limit)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result