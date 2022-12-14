from processor.dataprocessor_service import DataProcessorService


"""
    Main-модуль, т.е. модуль запуска приложений ("точка входа" приложения)
"""

if __name__ == '__main__':
    # Без указания полного пути, программа будет читать файл из своей корневой папки
    service = DataProcessorService("C:/datasets/neighbourhoods.csv", "C:/datasets/green_spaces.csv",
                         "C:/datasets/crime_rates.csv", db_connection_url="sqlite:///database.db")
    service.run_service()