from .dataprocessor_factory import DataProcessorFactory

"""
    В данном модуле реализуется класс с основной бизнес-логикой приложения. 
    Обычно такие модули / классы имеют в названии слово "Service".
"""


class DataProcessorService:

    def __init__(self, datasource_neighbourhoods: str, datasource_parks: str, datasource_crime_rates: str):
        self.datasource = []
        self.datasource.append(datasource_neighbourhoods)
        self.datasource.append(datasource_parks)
        self.datasource.append(datasource_crime_rates)
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
        for i in range(3):
            processor = self.processor_fabric[i].get_processor(self.datasource[i])        # Инициализируем обработчик
            if processor is not None:
                processor.run()
                processor.print_result()
            else:
                print('Nothing to run')
