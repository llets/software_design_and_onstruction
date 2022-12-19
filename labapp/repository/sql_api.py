from typing import List
from .connector import StoreConnector

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.

    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""
# select from source files
def select_all_from_source_file_neighbourhoods(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files WHERE id = 1 ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_source_file_parks(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files WHERE id = 2 ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_source_file_crime_rate(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files WHERE id = 3 ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    return result

# select all from points
def select_all_from_points_neighbourhoods(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM points_neighbourhoods ORDER BY nameN ASC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_points_parks(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM points_parks ORDER BY nameP ASC'
    result = connector.execute(query).fetchall()
    return result

# select all from neigh, parks or crime rates
def select_all_from_neighbourhoods(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM processed_neighbourhoods ORDER BY nameP ASC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_parks(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM processed_parks ORDER BY nameP ASC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_crime_rates(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM processed_crime_rates ORDER BY nameN ASC'
    result = connector.execute(query).fetchall()
    return result

# select points of certain neigh or park
def select_points_of_certain_neighbourhood(connector: StoreConnector, i)-> List[tuple]:
    query = f'SELECT * FROM points_neighbourhoods WHERE parent_id = {i}'
    result = connector.execute(query).fetchall()
    return result

def select_points_of_certain_park(connector: StoreConnector, i)-> List[tuple]:
    query = f'SELECT * FROM points_parks WHERE parent_id = {i}'
    result = connector.execute(query).fetchall()
    return result

# select neigh with high crime rate
def select_high_from_crime_rates(connector: StoreConnector, limit: int = None)-> List[tuple]:
    result = []
    if limit is None:
        query = f'SELECT nameN, average_rate ' \
                f'FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3*2 AS second_avg_table ' \
                f'      FROM processed_crime_rates) ' \
                f'      ON average_rate >= second_avg_table'
        result = connector.execute(query).fetchall()
    else:
        query = f'SELECT nameN, average_rate ' \
                f'FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3*2 AS second_avg_table ' \
                f'      FROM processed_crime_rates LIMIT {limit}) ' \
                f'      ON average_rate >= second_avg_table LIMIT {limit}'
        result = connector.execute(query).fetchall()
    return result
    # query = f'SELECT nameN, average_rate ' \
    #         f'FROM processed_crime_rates ' \
    #         f'JOIN ' \
    #         f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3*2 AS second_avg_table ' \
    #         f'      FROM processed_crime_rates) ' \
    #         f'      ON average_rate >= second_avg_table'
    # result = connector.execute(query).fetchall()
    # return result

# select neigh with middle crime rate
def select_middle_from_crime_rates(connector: StoreConnector, limit: int = None)-> List[tuple]:
    result = []
    if limit is None:
        query = f'SELECT nameN, average_rate FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT( ' \
                f'             ((MAX(average_rate)+MIN(average_rate))/3) AS first_avg_table, ' \
                f'             (((MAX(average_rate)+MIN(average_rate))/3*2) AS second_avg_table ' \
                f'      FROM processed_crime_rates) ' \
                f'      ) ON average_rate < second_avg_table AND average_rate >= first_avg_table LIMIT {limit}'
        result = connector.execute(query).fetchall()
    else:
        query = f'SELECT nameN, average_rate FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT( ' \
                f'             ((MAX(average_rate)+MIN(average_rate))/3) AS first_avg_table, ' \
                f'             (((MAX(average_rate)+MIN(average_rate))/3*2) AS second_avg_table ' \
                f'      FROM processed_crime_rates)' \
                f'      ) ON average_rate < second_avg_table AND average_rate >= first_avg_table LIMIT {limit}'
        result = connector.execute(query).fetchall()
    return result
    # query = f'SELECT nameN, average_rate FROM processed_crime_rates ' \
    #         f'JOIN ' \
    #         f'      (SELECT( ' \
    #         f'             ((MAX(average_rate)+MIN(average_rate))/3) AS first_avg_table, ' \
    #         f'             (((MAX(average_rate)+MIN(average_rate))/3*2) AS second_avg_table ' \
    #         f'      FROM processed_crime_rates) ' \
    #         f'      ) ' \
    #         f'ON average_rate >= first_avg_table AND average_rate < second_avg_table'
    # result = connector.execute(query).fetchall()
    # return result

# select neigh with low crime rate
def select_low_from_crime_rates(connector: StoreConnector, limit: int = None)-> List[tuple]:
    """ Выборка строк из таблицы с обработанными данными """
    result = []
    if limit is None:
        query = f'SELECT nameN, average_rate ' \
                f'FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3 AS first_avg_table ' \
                f'      FROM processed_crime_rates) ' \
                f'      ON average_rate < first_avg_table'
        result = connector.execute(query).fetchall()
    else:
        query = f'SELECT nameN, average_rate ' \
                f'FROM processed_crime_rates ' \
                f'JOIN ' \
                f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3 AS first_avg_table ' \
                f'      FROM processed_crime_rates) ' \
                f'      ON average_rate < first_avg_table LIMIT {limit}'
        result = connector.execute(query).fetchall()
    return result
    # query = f'SELECT nameN, average_rate ' \
    #         f'FROM processed_crime_rates ' \
    #         f'JOIN ' \
    #         f'      (SELECT (MAX(average_rate)+MIN(average_rate))/3 AS first_avg_table ' \
    #         f'      FROM processed_crime_rates) ' \
    #         f'      ON average_rate < first_avg_table'
    # result = connector.execute(query).fetchall()
    # return result


def select_rows_from_processed_data(connector: StoreConnector, limit: int = None) -> List[tuple]:
    """ Выборка строк из таблицы с обработанными данными """
    result = []
    if limit is None:
        result = connector.execute(f"SELECT * FROM processed_crime_rates ORDER BY nameN ASC").fetchall()
    else:
        result = connector.execute(
            f"SELECT * FROM processed_crime_rates ORDER BY nameN ASC LIMIT {limit}").fetchall()
    return result