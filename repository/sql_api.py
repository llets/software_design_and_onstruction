from typing import List

import pandas

from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime
from collections import defaultdict

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.

    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""

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

def insert_into_source_files(connector: StoreConnector, filenames: [str]):
    """ Вставка в таблицу обработанных файлов """
    result = []
    for i in range(len(filenames)):
        now = datetime.now()  # текущая дата и время
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")  # преобразуем в формат SQL
        query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filenames[i]}\', \'{date_time}\')'
        result.append(connector.execute(query))
    return result

def insert_rows_into_processed_data(connector: StoreConnector, dataframe: [DataFrame]):
    """ Вставка строк из DataFrame в БД с привязкой данных к последнему обработанному файлу (по дате) """

    #1
    rows = dataframe[0].to_dict('records')
    files_list = select_all_from_source_file_neighbourhoods(connector)  # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple,
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами

    row_points = dataframe[0]['geometry']
    row_points_splitted = []

    points_res = []
    rows_res = []
    if len(files_list) > 0:
        for i in range(len(row_points)):
            flag_okay_point = True
            row_points_splitted.append(row_points.iloc[i].split('), ('))
            row_points_splitted[i][0] = row_points_splitted[i][0].removeprefix('(')
            row_points_splitted[i][len(row_points_splitted[i])-1] = row_points_splitted[i][len(row_points_splitted[i])-1].removesuffix(')')
            for point in row_points_splitted[i]:
                if (point.find(')') != -1 or
                point.find('(') != -1):
                    flag_okay_point = False
                    break
            if (flag_okay_point == True):
                rows_res.append(rows[i]['AREA_NAME'])
                points_res.append(row_points_splitted[i])

        for i in range(len(rows_res)):
            print(1)
            connector.execute(
                f'INSERT INTO processed_neighbourhoods (nameN, source_file) '
                f'VALUES (\'{rows_res[i]}\', {last_file_id})')

        for i in range(len(points_res)):
            dict = {}
            dict ["latt"] = []
            dict ["long"] = []
            for j in range(len(points_res[i])):
                dict ["latt"].append(float(points_res[i][j].split(',')[0]))
                dict ["long"].append(float(points_res[i][j].split(',')[1]))
            for j in range (len(points_res[i])):
                print(1.1)
                connector.execute(
                    f'INSERT INTO points_neighbourhoods (parent_id, lattit, longit) '
                    f'VALUES ({i}, {dict["latt"][j]}, {dict["long"][j]})')
        print('Points and Neigh was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')

    #2
    rows = dataframe[1].to_dict('records')
    files_list = select_all_from_source_file_parks(connector)  # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple,
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами

    row_points = dataframe[1]['geometry']
    row_points_splitted = []

    points_res = []
    rows_res = []
    if len(files_list) > 0:
        for i in range(len(row_points)):
            flag_okay_point = True
            row_points_splitted.append(row_points.iloc[i].split('), ('))
            row_points_splitted[i][0] = row_points_splitted[i][0].removeprefix('(')
            row_points_splitted[i][len(row_points_splitted[i])-1] = row_points_splitted[i][len(row_points_splitted[i])-1].removesuffix(')')
            for point in row_points_splitted[i]:
                if (point.find(')') != -1 or
                point.find('(') != -1):
                    flag_okay_point = False
                    break
            if (flag_okay_point == True):
                rows_res.append(rows[i]['AREA_NAME'])
                points_res.append(row_points_splitted[i])

        for i in range(len(rows_res)):
            connector.execute(
                f'INSERT INTO processed_parks (nameP, source_file) '
                f'VALUES (\'{rows_res[i]}\', {last_file_id})')

        for i in range(len(points_res)):
            dict = {}
            dict ["latt"] = []
            dict ["long"] = []
            for j in range(len(points_res[i])):
                dict ["latt"].append(float(points_res[i][j].split(',')[0]))
                dict ["long"].append(float(points_res[i][j].split(',')[1]))
            for j in range (len(points_res[i])):
                print(2.1)
                connector.execute(
                    f'INSERT INTO points_parks (parent_id, lattit, longit) '
                    f'VALUES ({i}, {dict["latt"][j]}, {dict["long"][j]})')
        print('Points and Parks was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')

    #3
    rows = dataframe[2].to_dict('records')
    files_list = select_all_from_source_file_crime_rate(connector)  # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами
    if len(files_list) > 0:
        for row in rows:
            connector.execute(
                f'INSERT INTO processed_crime_rates (nameN, average_rate, source_file) '
                f'VALUES (\'{row["Neighbourhood"]}\', \'{row["avg_crime"]}\', {last_file_id})')
        print('Crime rate was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')

def select_all_from_points_neighbourhoods(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM points_neighbourhoods ORDER BY nameN ASC'
    result = connector.execute(query).fetchall()
    return result

def select_all_from_points_parks(connector: StoreConnector)-> List[tuple]:
    query = f'SELECT * FROM points_parks ORDER BY nameP ASC'
    result = connector.execute(query).fetchall()
    return result

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

def select_points_of_certain_neighbourhood(connector: StoreConnector, i)-> List[tuple]:
    query = f'SELECT * FROM points_neighbourhoods WHERE parent_id = {i}'
    result = connector.execute(query).fetchall()
    return result

def select_points_of_certain_park(connector: StoreConnector, i)-> List[tuple]:
    query = f'SELECT * FROM points_parks WHERE parent_id = {i}'
    result = connector.execute(query).fetchall()
    return result