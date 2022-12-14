from pandas import DataFrame
from .dataprocessor_factory import DataProcessorFactory
from repository.connectorfactory import SQLStoreConnectorFactory       # подключаем фабрику коннекторов БД
from repository.sql_api import *                                       # подключаем API для работы с БД

"""
    В данном модуле реализуется класс с основной бизнес-логикой приложения. 
    Обычно такие модули / классы имеют в названии слово "Service".
"""


class DataProcessorService:

    def __init__(self, datasource_neighbourhoods: str, datasource_parks: str, datasource_crime_rates: str,
                 db_connection_url: str):
        self.datasource = []
        self.datasource.append(datasource_neighbourhoods)
        self.datasource.append(datasource_parks)
        self.datasource.append(datasource_crime_rates)
        self.db_connection_url = db_connection_url
        self.processor_fabric = []
        for i in range(3):
            self.processor_fabric.append(DataProcessorFactory())

    """
        ВАЖНО! Обратите внимание, что метод run_service использует только методы базового абстрактного класса DataProcessor
        и, таким образом, будет выполняться для любого типа обработчика данных (CSV или TXT), что позволяет в дальнейшем 
        расширять приложение, просто добавляя другие классы обработчиков, которые, например, работают с базой данных или
        сетевым хранилищем файлов (например, FTP-сервером).
    """

    def run_service(self) -> None:
        """ Метод, который запускает сервис обработки данных  """
        processor = []
        for i in range(3):
            processor.append(self.processor_fabric[i].get_processor(self.datasource[i])) # Инициализируем обработчик
        for i in range(3):
            if processor[i] is not None:
                processor[i].run()
                processor[i].print_result()
            else:
                print('Nothing to run')
        # после завершения обработки, запускаем необходимые методы для работы с БД
        results = []
        for i in range(3):
            results.append(processor[i].result)
        self.save_to_database(results)

    def save_to_database(self, results: [DataFrame]) -> None:
        """ Сохранение данных в БД """
        db_connector = None
        if results is not None:
            try:
                db_connector = SQLStoreConnectorFactory().get_connector(self.db_connection_url)  # инициализируем соединение
                db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
                insert_into_source_files(db_connector, self.datasource)  # сохраняем в БД информацию о новом файле с набором данных
                print(select_all_from_source_file_neighbourhoods(db_connector))  # вывод списка всех обработанных файлов
                print(select_all_from_source_file_parks(db_connector))
                print(select_all_from_source_file_crime_rate(db_connector))
                insert_rows_into_processed_data(db_connector, results)  # записываем в БД результат обработки набора данных
            except Exception as e:
                print(e)
            finally:
                if db_connector is not None:
                    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
                    db_connector.close()            # Завершаем работу с БД