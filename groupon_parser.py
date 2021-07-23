#!/usr/bin/python3

import requests
from bs4 import BeautifulSoup
import cloudscraper
from pprint import pprint
import re
from random import choice
import logging.config
from db_conn_module import *
import json
from datetime import datetime, date
from time import strftime, gmtime
from proxy_module import *
import cgi
import os
import traceback
from logging_module import add_log
import time
import sys
import html
from commons import *

print('Content-Type: text/html')
print()

logger = add_log  # создаем объект для записывания логов

SITE_ID = 2

get_coupon_name = False
get_coupon_description = False
get_coupon_code = False
get_coupon_url = False
get_coupon_expiration_date = False

try:
    current_day_date = date.today()
    current_time = datetime.now().time()
    current_time = str(current_time).split('.')[0]  # текущ время без добавления милисекунд

    # if not os.path.exists(f'logs/{current_day_date}'):
    #     os.mkdir(f'logs/{current_day_date}', mode=0o777)     # создаем директорию для сегодняшней даты

    # logs = open(f'logs/{current_day_date}/{current_time}.txt', 'w+', encoding='utf-8')  # создание лог файла
except Exception as er:
    logger(er)

try:
    # connection to DATABASE
except Exception as er:
    print(er)
    logger(er)


def is_exist_coupon(connection, brand_id, coupon):
    if connection is False:
        return False
    if brand_id is False:
        return False
    if coupon is False:
        return False

    values = (coupon['code'], coupon['title'], coupon['description'])

    sql_query = """SELECT id FROM coupon_parsing
                WHERE code = %s 
                AND name = %s
                AND description = %s
                """
    rows = execute_fetch_prepared(connection, sql_query, values)

    if len(rows) <= 0:
        return True
    if len(rows) > 0:
        return False


def insert_coupon(connection, brand_id, coupon):
    if connection is False:
        return False
    if brand_id is False:
        return False
    if coupon is False:
        return False


    values = (coupon['code'], SITE_ID ,brand_id, coupon['title'], coupon['description'], coupon['url'], coupon['expiration_date'], coupon['amount'], coupon['type'])

    sql_insert = """INSERT INTO coupon_parsing(code, site_id, brand_id, name, description, special_url, expiration_date, amount, type)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    execute_query(connection, sql_insert, values)


def daily_report_generate():
    if os.path.exists('daily_reports/report.html'):
        old_name_report = os.path.join('daily_reports', 'report.html')
        new_name_report = os.path.join('daily_reports',
                                       f'report_{strftime("%d-%m-%y", gmtime(os.path.getmtime(old_name_report)))}.html')
        if os.path.exists(new_name_report):
            os.remove(new_name_report)
        os.rename(old_name_report, new_name_report)

    with open(f'daily_reports/report.html', 'w+', encoding='utf-8') as daily_report:

        daily_report.write(start_sql_query)
        if get_coupon_name:
            daily_report.write(
                f'<br><font color="green">Got <b>coupon name</b> from GROUPON: {get_coupon_name}</font>')
        if not get_coupon_name:
            daily_report.write(
                f'<br><font color="red">Got <b>coupon name</b> from GROUPON: {get_coupon_name}</font>'
        )
        if get_coupon_description:
            daily_report.write(
                f'<br><font color="green">Got <b>coupon description</b> from GROUPON: {get_coupon_description}</font>'
            )
        if not get_coupon_description:
            daily_report.write(
                f'<br><font color="red">Got <b>coupon description</b> from GROUPON: {get_coupon_description}</font>'
            )
        if get_coupon_code:
            daily_report.write(
                f'<br><font color="green">Got <b>coupon code</b> from GROUPON: {get_coupon_code}</font>'
            )
        if not get_coupon_code:
            daily_report.write(
                f'<br><font color="red">Got <b>coupon code</b> from GROUPON: {get_coupon_code}</font>'
            )
        if get_coupon_url:
            daily_report.write(
                f'<br><font color="green">Got <b>coupon url</b> from GROUPON: {get_coupon_url}</font>'
            )
        if not get_coupon_url:
            daily_report.write(
                f'<br><font color="red">Got <b>coupon url</b> from GROUPON: {get_coupon_url}</font>'
            )
        if get_coupon_expiration_date:
            daily_report.write(
                f'<br><font color="green">Got <b>coupon expiration date</b> from GROUPON: {get_coupon_expiration_date}</font>'
            )
        if not get_coupon_expiration_date:
            daily_report.write(
                f'<br><font color="red">Got <b>coupon expiration date</b> from GROUPON: {get_coupon_expiration_date}</font>'
            )


stop = open('stop.txt', 'w+')
stop.close()

try:

    start_time = datetime.now()

    form = cgi.FieldStorage()

    try:
        if form['prints'].value == 'on':
            print_on_page = True  # устанавливаем флаг тру, если включен чекбокс
    except KeyError:
        print_on_page = False  # выключение чекбокса словить не можем, поэтому при ошибке(пустом чекбоксе) ставим фолс

    if print_on_page:
        print('<title>Test Parse GROUPON</title>')
        print('<h1>GROUPON Parser</h1>')

    try:
        if form['daily_tests'].value == 'on':
            daily_tests = True
    except KeyError:
        daily_tests = False

    request_field = form['TEXT'].value

    if request_field.isdigit():
        query = form['TEXT'].value
        start_sql_query = "SELECT id FROM brands WHERE id = %s"
        selected_brands = execute_fetch(connection, start_sql_query, [int(query)])  # обязательно преобразуем в инт


    elif 'SELECT' in request_field:
        start_sql_query = form['TEXT'].value
        if start_sql_query.startswith('SELECT *'):
            start_sql_query = start_sql_query.replace('SELECT *',
                                                      'SELECT id')  # заменяем * на id, не храним весь запрос в переменной
        selected_brands = execute_fetch(connection, start_sql_query)

    sel_business = []

    for id in selected_brands:  # достаем айдишники из кортежа
        sel_business.append(id[0])

    count_changes_proxy = 0  # если 10 проксей перебрали - переходим на другой бизнес
    count_proxy_error = 0  # если 50 проксей перебрали - останавливаем скрипт и пишем логи
    count_passed_biz = 0
    count_captcha2 = 0  # для прохождения капчи

    len_all_biz = len(sel_business)

    while len(sel_business) > 0:

        if not os.path.exists('stop.txt'):
            if print_on_page:
                print('file deleted<br>')
                print('<h2>Script stopped</h2>')
            sys.exit(0)

        brand_id = sel_business.pop(0)

        values = (int(brand_id), SITE_ID)
        sql_query = 'SELECT business_url FROM businesses_info WHERE brand_id = %s AND site_id = %s'
        selected_url = execute_fetch_prepared(connection, sql_query, values)

        if selected_url == [] or selected_url is None:
            continue

        if print_on_page:
            print(f"<br>Current business id - {brand_id}<br><br>")

        selected_url = selected_url[0][0]
        if print_on_page:
            print(f"<br>{selected_url}<br>")

        try:  # Более общий экзепшн(но не верхнего уровня)

            # urltest = "http://firstbankmi.com/mcfreyze/request_info.php"

            # ------------------- НАЧАЛО РАБОТЫ С ПРОКСИ -----------------------

            proxies_info = create_proxy_list()  # создаем список проксей
            random_proxy_index = get_not_banned_proxy(proxies_info=proxies_info)[1]  # получаем рандомный прокси

            create_proxy_local_file('local')  # создаем локальный файл с прокси

            random_proxy_json = load_data('logs_local_proxy.json')  # открываем джсон (в котором есть бан прокси)

            random_proxy = get_not_banned_proxy(random_proxy_json)[0]  # получаем не забаненую проксю

            proxies = {
                'http': f'http://{random_proxy["username"]}:{random_proxy["password"]}@{random_proxy["ip"]}',
                'https': f'http://{random_proxy["username"]}:{random_proxy["password"]}@{random_proxy["ip"]}'
            }

            # --------------------- КОНЕЦ РАБОТЫ С ПРОКСИ -----------------------

            scraper = cloudscraper.create_scraper()  # создали объект cloudscaper https://pypi.org/project/cloudscraper/

            try:  # пробуем сделать гет запрос

                # ---------------------- НАЧАЛО ГЕТ ЗАПРОСА и обработки его ОШИБОК --------------

                if print_on_page:
                    print(f'<br>Current IP address - {random_proxy["ip"]}<br><br>')
                # cookie_value, user_agent = cloudscraper.get_cookie_string(selected_url)
                #
                # print('<br>GET / HTTP/1.1\nCookie: {}\n<br>User-Agent: {}\n<br><br>'.format(cookie_value, user_agent))

                html_doc = scraper.get(selected_url, timeout=10, proxies=proxies)
                # html_doc = scraper.get("https://www.manta.com/c/mrybs6k/dillon-painting", timeout=10, proxies=proxies).text

            except requests.exceptions.ConnectTimeout as er:
                if count_changes_proxy == 10:  # если счетчик доходит до n перебраных проксей
                    count_changes_proxy = 0
                    continue  # пропускаем этот бизнес, не делаем его инсерт в начало массива, а идем по следующим

                count_changes_proxy += 1
                proxyerror_message = f'\nProxy error - {er}\n'
                ban_proxy(proxies_info=proxies_info, random_proxy_index=random_proxy_index, log_name='local')
                sel_business.insert(0, brand_id)
                if print_on_page:
                    print(proxyerror_message)
                continue


            except requests.exceptions.ProxyError as er:
                if count_changes_proxy == 10:  # если счетчик доходит до n перебраных проксей
                    count_changes_proxy = 0
                    continue  # пропускаем этот бизнес, не делаем его инсерт в начало массива, а идем по следующим
                if count_proxy_error == 50:  # если 50 проксей выдают еррор, выбиваем прогу и записываем логи
                    print_proxy_error = f'Script down after {count_proxy_error} ProxyError\n\n'
                    logger(print_proxy_error)
                    if print_on_page:
                        print(print_proxy_error)
                    sys.exit(0)

                count_proxy_error += 1
                count_changes_proxy += 1

                proxyerror_message = f'<br><br>Proxy error - {er}<br><br>'
                ban_proxy(proxies_info=proxies_info, random_proxy_index=random_proxy_index, log_name='local')
                sel_business.insert(0, brand_id)
                if print_on_page:
                    print(proxyerror_message)
                continue

            except cloudscraper.exceptions.CloudflareChallengeError as er:
                logger(er)
                if print_on_page:
                    print(f"Captcha Error - {er}")

                if count_captcha2 == 3:  # трижды получив капчу на одном бизнесе, пропускаем его
                    count_captcha2 = 0
                    continue

                count_captcha2 += 1  # обновляем каунт

                sel_business.insert(0, brand_id)  # добавляем его по новой в список
                continue

            except Exception:
                logger("Business id - " + str(brand_id) + "\n" + traceback.format_exc())
                if print_on_page:
                    print(traceback.format_exc())
                continue

            count_proxy_error = 0  # обновляем счетчики
            count_changes_proxy = 0
            count_captcha2 = 0

            # --------------------- НАЧИНАЕМ ПАРСИНГ HTML СТРАНИЦЫ ------------------------

            if print_on_page:
                print(f"<br>Get req to - {selected_url}<br>")
                print(f'<br>Response code - {html_doc.status_code}<br>')

            # https://regex101.com/r/WIQpvH/2
            regex = r"\"coupons\":(\[.*?\"VC_MerchantUserOfferWithBranch\"}])"
            match = re.search(regex, html_doc.text)

            coupons = []

            # count = 0
            if match is not None:
                coupons_jsn = match.group(1)

                coupons_jsn = json.loads(coupons_jsn)

                for coupon in coupons_jsn:
                    # if count == 5: # временно, пока тестируем через веб
                    #     break
                    coup = dict()
                    try:
                        coup['title'] = coupon['offerTitle']

                        # daily_test
                        get_coupon_name = True

                        if coup['title'] is None:
                            coup['title'] = 'None'
                    except:
                        coup['title'] = 'None'
                    try:
                        coup['description'] = coupon['offerDescription']

                        # daily_test
                        get_coupon_description = True

                        if coup['description'] is None:
                            coup['description'] = 'None'
                    except:
                        coup['description'] = 'None'

                    offer_id = coupon['offerId']

                    # offer_type = coupon['offerType']
                    try:
                        url_code = 'https://www.groupon.com/coupons/redemption/' + str(offer_id)
                        second_get = scraper.get(url_code, timeout=10, proxies=proxies)
                        var = second_get.json()
                        offer_code = var['offerCode']
                        coup['code'] = offer_code

                        # daily_test
                        get_coupon_code = True

                        if coup['code'] is None:
                            coup['code'] = 'None'
                    except:
                        coup['code'] = 'None'

                    try:
                        merchantId = var['merchantId']
                        third_get = scraper.get('https://www.groupon.com/coupons/redirect/merchant/' + str(merchantId), timeout=10, proxies=proxies)
                        coup['url'] = third_get.request.url

                        get_coupon_url = True

                        if coup['url'] is None:
                            coup['url'] = 'None'
                    except:
                        coup['url'] = 'None'

                    try:
                        date = var['expiryDateTime']

                        if date is None:
                            coup['expiration_date'] = 'None'
                        elif date == 'Null':
                            coup['expiration_date'] = 'None'

                        elif date is not None:

                            date = date.split('.')[0]
                            coup['expiration_date'] = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").timestamp()

                            # daily_test
                            get_coupon_expiration_date = True

                    except:
                        coup['expiration_date'] = 'None'

                    try:
                        # ищем в названии
                        if '$' in str(coup['title']):
                            coup['type'] = '$'
                        elif '%' in str(coup['title']):
                            coup['type'] = '%'
                        elif 'ship' in str(coup['title']) or 'shipping' in str(coup['title']):
                            coup['type'] = 'FS'
                        else:
                            coup['type'] = 'DEAL'

                        if coup['type'] == 'DEAL':
                            # ищем в описании
                            if '$' in str(coup['description']):
                                coup['type'] = '$'
                            elif '%' in str(coup['description']):
                                coup['type'] = '%'
                            elif 'ship' in str(coup['description']) or 'shipping' in str(coup['description']):
                                coup['type'] = 'FS'
                            else:
                                coup['type'] = 'DEAL'
                    except:
                        coup['type'] = ''

                    try:
                        # https://regex101.com/r/5NQHCp/1
                        if coup['type'] == '$':
                            regex = r"\$([0-9,.]+)"  # ищем любое число(первое вхождение)
                            match = re.search(regex, coup['title'])
                            coup['amount'] = ''
                            if match is not None:
                                coup['amount'] = match.group(1)
                            elif match is None:
                                match = re.search(regex, coup['description'])

                                if match is not None:
                                    coup['amount'] = match.group(1)
                                else:
                                    coup['amoumt'] = ''

                            else:
                                coup['amount'] = ''

                        elif coup['type'] == '%':
                            regex = r"([0-9,.]+%)"  # ищем любое число(первое вхождение)
                            match = re.search(regex, coup['title'])
                            coup['amount'] = ''
                            if match is not None:
                                coup['amount'] = match.group(1)
                            elif match is None:
                                match = re.search(regex, coup['description'])

                                if match is not None:
                                    coup['amount'] = match.group(1)
                                else:
                                    coup['amoumt'] = ''

                            else:
                                coup['amount'] = ''

                        else:
                            coup['amount'] = ''

                    except:
                        coup['amount'] = ''

                    coupons.append(coup)
                    # count += 1

            # if coupons:
            #     for c in coupons:
            #         if print_on_page:
            #             print("<b>COUPON</b>")
            #             print(f'<br>Name coupon - {c["title"].encode("utf-8")}<br>')
            #             print(f'<br>Description coupon - {c["description"].encode("utf-8")}<br>')
            #             print(f'<br>Code coupon - {c["code"]}<br>')
            #             print(f'<br>Url coupon - {c["url"]}<br>')
            #             print(f'<br>Expiration date coupon - {c["expiration_date"]}<br>')
            #             print("<br><hr>")

            if coupons:
                for c in coupons:
                    c['title'] = c['title'].replace('None', '')
                    c['description'] = c['description'].replace('None', '')
                    c['code'] = c['code'].replace('None', '')
                    c['url'] = c['url'].replace('None', '')
                    # if not c['expiration_date'].isdigit():
                    if 'None' in str(c['expiration_date']):
                        c['expiration_date'] = str(c['expiration_date']).replace('None', '')

                    if c['amount'] == ',' or c['amount'] == '.':
                        c['amount'] = c['amount'].replace(',', '').replace('.', '')

                    c['amount'] = c['amount'].replace('2021', '').replace('2022', '').replace('2019', '').replace(
                        '2020', '')

                    if c['amount'] != '':
                        if c['amount'][-1] == '.' or c['amount'][-1] == ',':
                            c['amount'] = c['amount'][:-1]

            if coupons:
                for c in coupons:
                    if print_on_page:
                        print("<b>COUPON</b>")
                        print(f'<br>Name coupon - {c["title"].encode("utf-8")}<br>')
                        print(f'<br>Description coupon - {c["description"].encode("utf-8")}<br>')
                        print(f'<br>Code coupon - {c["code"]}<br>')
                        print(f'<br>Url coupon - {c["url"]}<br>')
                        print(f'<br>Expiration date coupon - {c["expiration_date"]}<br>')
                        print(f'<br>Type coupon - {c["type"]}<br>')
                        print(f'<br>Amount coupon - {c["amount"]}<br>')


                    if is_exist_coupon(connection, brand_id, c):
                        insert_coupon(connection, brand_id, c)

                        if print_on_page:
                            print("<br><br>COUPON WAS ADD TO DATABASE<br><br>")
                    else:
                        if print_on_page:
                            print("<br><br>THIS COUPON EXIST<br><br>")
                    if print_on_page:
                        print("<br><hr>")


            else:
                if print_on_page:
                    print("<br><b>We didn't find coupons!</b><br>")
                    print("<br><hr>")

            try:
                count_passed_biz += 1
                business_left = len_all_biz - count_passed_biz
                status_info = {"START REQUEST: ": start_sql_query,
                               "All you need to go through: ": len_all_biz,
                               "Current id: ": brand_id,
                               "Passed: ": count_passed_biz,
                               "Brands left: ": business_left,
                               "Thread id: ": connection.connection_id}

                with open(
                        f'logs/status_{current_time}.json',
                        'w+') as f:
                    json.dump(status_info, f, indent=4)

            except Exception:
                logger("Business id - " + str(brand_id) + "\n" + traceback.format_exc())
                if print_on_page:
                    print("Business id - " + str(brand_id) + "\n" + traceback.format_exc())
                continue

        except Exception:
            logger(traceback.format_exc())
            if print_on_page:
                print(traceback.format_exc())

    if daily_tests:
        daily_report_generate()

    if print_on_page:
        print("<br>End of parsing<br>")

except Exception:
    logger(traceback.format_exc())
    if print_on_page:
        print(traceback.format_exc())