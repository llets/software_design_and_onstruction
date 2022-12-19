# Подключаем объект приложения Flask из __init__.py
from labapp import app
# Подключаем библиотеку для "рендеринга" html-шаблонов из папки templates
from flask import render_template, make_response, request, jsonify

import labapp.webservice as webservice   # подключаем модуль с реализацией бизнес-логики обработки запросов

"""
    Модуль регистрации обработчиков маршрутов, т.е. здесь реализуется обработка запросов
    при переходе пользователя на определенные адреса веб-приложения
"""


@app.route('/', methods=['GET'])
@app.route('/Homepage.html', methods=['GET'])
def index():
    """ Обработка запроса к индексной странице """
    # Пример вызова метода с выборкой данных из БД и вставка полученных данных в html-шаблон
    processed_data = webservice.get_processed_crime_rates(5);

    # "рендеринг" (т.е. вставка динамически изменяемых данных) в шаблон index.html и возвращение готовой страницы
    return render_template('Homepage.html', processed_data=processed_data)
    # return render_template('Homepage.html', parentid = processed_points[1], datalat= processed_points[2], datalng = processed_points[3])
#     return render_template('data.html',
#                            title='MY BEST WEBSERVICE!!1',
#                            page_name=f'DATA_FILE_{source_file_id}',
#                            navmenu=webservice.navmenu,
#                            processed_data=processed_data)

@app.route('/get_result', methods=['POST'])
def get_result():
    """ Обработка запроса к индексной странице """
    # Пример вызова метода с выборкой данных из БД и вставка полученных данных в html-шаблон
    crime_rate = request.form['select'];
    if (crime_rate == "High"):
        processed_data = webservice.get_high_crime_rates(5);
    elif (crime_rate == "Middle"):
        processed_data = webservice.get_middle_crime_rates(5);
    else:
        processed_data = webservice.get_low_crime_rates(5);
    # "рендеринг" (т.е. вставка динамически изменяемых данных) в шаблон index.html и возвращение готовой страницы
    return render_template('Homepage.html', processed_data=processed_data)
    # return render_template('Homepage.html', parentid = processed_points[1], datalat= processed_points[2], datalng = processed_points[3])
#     return render_template('data.html',
#                            title='MY BEST WEBSERVICE!!1',
#                            page_name=f'DATA_FILE_{source_file_id}',
#                            navmenu=webservice.navmenu,
#                            processed_data=processed_data)

@app.route('/Contact.html', methods=['GET'])
def contact():
    """ Обработка запроса к странице contact.html """
    return render_template('Contact.html')

@app.route('/About-us.html', methods=['GET'])
def about():
    """ Обработка запроса к странице contact.html """
    return render_template('About-us.html')

# @app.route('/data/<int:source_file_id>', methods=['GET'])
# def get_data(source_file_id: int):
#     """ Вывод данных по идентификатору обработанного файла """
#     processed_data = webservice.get_processed_data(source_file=source_file_id)
#     return render_template('data.html',
#                            title='MY BEST WEBSERVICE!!1',
#                            page_name=f'DATA_FILE_{source_file_id}',
#                            navmenu=webservice.navmenu,
#                            processed_data=processed_data)


# @app.route('/api/contactrequest', methods=['POST'])
# def post_contact():
#     """ Пример обработки POST-запроса для демонстрации подхода AJAX (см. formsend.js и ЛР№5 АВСиКС) """
#     request_data = request.json     # получаeм json-данные из запроса
#     # Если в запросе нет данных или неверный заголовок запроса (т.е. нет 'application/json'),
#     # или в этом объекте, например, не заполнено обязательное поле 'firstname'
#     if not request_data or request_data['firstname'] == '':
#         # возвращаем стандартный код 400 HTTP-протокола (неверный запрос)
#         return bad_request()
#     # Иначе отправляем json-ответ с сообщением об успешном получении запроса
#     else:
#         msg = request_data['firstname'] + ", ваш запрос получен !"
#         return jsonify({'message': msg})


@app.route('/notfound', methods=['GET'])
def not_found_html():
    """ Возврат html-страницы с кодом 404 (Не найдено) """
    return render_template('404.html', title='404', err={'error': 'Not found', 'code': 404})


def bad_request():
    """ Формирование json-ответа с ошибкой 400 протокола HTTP (Неверный запрос) """
    return make_response(jsonify({'message': 'Bad request !'}), 400)
