from abc import ABC, abstractmethod     # подключаем инструменты для создания абстрактных классов
import pandas   # пакет для работы с датасетами

"""
    В данном модуле реализуются классы обработчиков для 
    применения алгоритма обработки к различным типам файлов (csv или txt).
    
    ВАЖНО! Если реализация различных обработчиков занимает большое 
    количество строк, то необходимо оформлять каждый класс в отдельном файле
"""


class DataProcessor(ABC):
    """ Родительский класс для обработчиков файлов """

    def __init__(self, datasource):
        # общие атрибуты для классов обработчиков данных
        self._datasource = datasource   # путь к источнику данных
        self._dataset = None            # входной набор данных
        self.result = None              # выходной набор данных (результат обработки)

    # Все методы, помеченные декоратором @abstractmethod, ОБЯЗАТЕЛЬНЫ для переобределения
    @abstractmethod
    def read(self) -> bool:
        """ Метод, инициализирующий источник данных """
        pass

    @abstractmethod
    def run(self) -> None:
        """ Точка запуска методов обработки данных """
        pass

    """
        Метод sort_data_by_col - пример одного из методов обработки данных.
        В данном случае метод просто сортирует входной датасет по наименованию 
        заданной колонки (аргумент colname) и устанвливает тип сортировки: 
        ascending = True - по возрастанию, ascending = False - по убыванию
        
        ВАЖНО! Следует логически разделять методы обработки, например, отдельный метод для сортировки, 
        отдельный метод для удаления "пустот" в датасете (очистка) и т.д. Это позволит гибко применять необходимые
        методы при переопределении метода run для того или иного типа обработчика.
        НАПРИМЕР, если ваш источник данных это не файл, а база данных, тогда метод сортировки будет не нужен,
        т.к. сортировку можно сделать при выполнении SQL-запроса типа SELECT ... ORDER BY...
    """

    def sort_data_by_col(self, df: pandas.DataFrame, colname: str, asc: bool) -> pandas.DataFrame:
            return df.sort_values(by=[colname], ascending=asc)

    def drop_columns(self, df: pandas.DataFrame, colname: [str]) -> pandas.DataFrame:
        new_dataset = df
        for i in range(len(colname)):
            new_dataset = new_dataset.drop(colname[i], axis=1)
        return new_dataset

    def choose_matching(self, df: pandas.DataFrame, colname: str, matching: str) -> pandas.DataFrame:
        if (not matching.startswith('!')):
            return df.loc[df[colname] == matching]
        else:
            return df.loc[df[colname] != matching[1:]]

    def delete_rows_with_null(self, df: pandas.DataFrame) -> pandas.DataFrame:
        return df.dropna()

    @abstractmethod
    def print_result(self) -> None:
        """ Абстрактный метод для вывода результата на экран """
        pass


class CsvDataProcessor(DataProcessor):
    """ Реализация класса-обработчика csv-файлов """

    def __init__(self, datasource):
        # Переопределяем конструктор родительского класса
        DataProcessor.__init__(self, datasource)    # инициализируем конструктор родительского класса для получения общих атрибутов
        self.separators = [',', ';', '|']        # список допустимых разделителей

    """
        Переопределяем метод инициализации источника данных.
        Т.к. данный класс предназначен для чтения CSV-файлов, то используем метод read_csv
        из библиотеки pandas
    """
    def read(self):
        try:
            # Пытаемся преобразовать данные файла в pandas.DataFrame, используя различные разделители
            for separator in self.separators:
                self._dataset = pandas.read_csv(self._datasource, sep=separator, header='infer', names=None, encoding="utf8", on_bad_lines= "warn")
                # Читаем имена колонок из файла данных
                col_names = self._dataset.columns
                # Если количество считанных колонок > 1 возвращаем True
                if len(col_names) > 1:
                    print(f'Columns read: {col_names} using separator {separator}')
                    return True
        except Exception as e:
            print(e)
        return False

    def run(self):
        match self._datasource:
            case "C:/datasets/neighbourhoods.csv":
                self.result = self.sort_data_by_col(self._dataset, "_id", True)
                self.result = self.drop_columns(self.result, ['AREA_ID', 'AREA_ATTR_ID', 'AREA_SHORT_CODE',
                                                    'AREA_LONG_CODE', 'OBJECTID', 'PARENT_AREA_ID', 'AREA_DESC',
                                                    'CLASSIFICATION', 'CLASSIFICATION_CODE'])
                print(pandas.DataFrame(self.result).nunique())
            case "C:/datasets/green_spaces.csv":
                print(pandas.DataFrame(self._dataset).nunique())
                self.result = self.sort_data_by_col(self._dataset, "_id", True)
                self.result = self.drop_columns(self.result, ['AREA_ID', 'AREA_ATTR_ID', 'AREA_SHORT_CODE',
                                                              'AREA_LONG_CODE','AREA_DESC','OBJECTID', 'PARENT_AREA_ID'])
                self.result = self.choose_matching(self.result, 'AREA_CLASS', '!OTHER_CEMETERY')
                self.result = self.delete_rows_with_null(self.result)
                self.result = self.drop_columns(self.result, ['AREA_CLASS_ID', 'AREA_CLASS'])
                print()
                self.result = self.result.loc[self.result['AREA_NAME'] != 'NaN']
                print(pandas.DataFrame(self.result).nunique())
            case "C:/datasets/crime_rates.csv":
                self.result = self.sort_data_by_col(self._dataset, "OBJECTID", True)
                self.result = self.drop_columns(self.result, ['Hood_ID', 'Population',
                    'Assault_2014','Assault_2015','Assault_2016','Assault_2017',
                    'Assault_2018','Assault_2019', 'Assault_Rate_2019','Assault_CHG',
                    'AutoTheft_2014','AutoTheft_2015','AutoTheft_2016','AutoTheft_2017',
                    'AutoTheft_2018','AutoTheft_2019','AutoTheft_Rate_2019','AutoTheft_CHG',
                    'BreakandEnter_2014','BreakandEnter_2015','BreakandEnter_2016','BreakandEnter_2017',
                    'BreakandEnter_2018','BreakandEnter_2019','BreakandEnter_CHG','BreakandEnter_Rate_2019',
                    'Homicide_2014','Homicide_2015','Homicide_2016','Homicide_2017',
                    'Homicide_2018', 'Homicide_2019', 'Homicide_CHG', 'Homicide_Rate_2019',
                    'Robbery_2014', 'Robbery_2015', 'Robbery_2016', 'Robbery_2017',
                    'Robbery_2018', 'Robbery_2019', 'Robbery_CHG', 'Robbery_Rate_2019',
                    'TheftOver_2014', 'TheftOver_2015', 'TheftOver_2016', 'TheftOver_2017',
                    'TheftOver_2018', 'TheftOver_2019', 'TheftOver_CHG', 'TheftOver_Rate_2019',
                    'Shape__Area','Shape__Length'])
                print(pandas.DataFrame(self.result).nunique())
            case _:
                print("Sorry, something went wrong with CSV name of file(s).\n")

    def print_result(self):
        print(f'Running CSV-file processor!\n', self.result)


class TxtDataProcessor(DataProcessor):
    """ Реализация класса-обработчика txt-файлов """

    def read(self):
        """ Реализация метода для чтения TXT-файла (разедитель колонок - пробелы) """
        try:
            self._dataset = pandas.read_table(self._datasource, sep='\s+', engine='python')
            col_names = self._dataset.columns
            if len(col_names) < 2:
                return False
            return True
        except Exception as e:
            print(str(e))
            return False

    def run(self):
        self.result = self.sort_data_by_col(self._dataset, "LKG", True)

    def print_result(self):
        print(f'Running TXT-file processor!\n', self.result)
