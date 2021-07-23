import json
from random import choice
from db_conn_module import *
import re
from logging_module import add_log

SITE_ID_9 = 9

logger = add_log  # создаем объект для записывания логов


def load_data(filename, encoding='utf-8'):
    with open(filename, encoding=encoding) as json_file:
        return json.load(json_file)


def create_random_name():
    char_list = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w', 'e', 'r', 't', 'y', 'u',
                 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b',
                 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G',
                 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M']
    name = ''
    while len(name) < 10:
        name = name + choice(char_list)
    return name


def clear_text(text):
    reg_exp = r'<[^>]*>'
    text = re.sub(reg_exp, '', text)
    text = re.sub(r'https?:\S+', ' ', text, flags=re.MULTILINE)
    text = text.replace('\\r', ' ')
    text = text.replace('\\n', ' ')
    text = text.replace('\\t', ' ')
    text = text.replace('\\f', ' ')
    text = text.replace('\\v', ' ')
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def get_city_url(connection, city_id):
    if connection is False:
        return False
    if city_id is False:
        return False

    sql_query = "SELECT city_url FROM cities WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [city_id])
    if len(rows) > 0:
        city_url = rows[0][0]
        return city_url
    else:
        return False


def get_state_abb(connection, state_id):
    if connection is False:
        return False
    if state_id is False:
        return False

    sql_query = "SELECT LOWER(abbreviation) FROM states WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [state_id])
    if len(rows) > 0:
        abbr = rows[0][0]
        return abbr
    else:
        return False


def get_city_id(connection, id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT city FROM businesses WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [id])
    if len(rows) > 0:
        city_id = rows[0][0]
        return city_id
    else:
        return False


def get_state_id(connection, id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT state FROM businesses WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [id])
    if len(rows) > 0:
        state_id = rows[0][0]
        return state_id
    else:
        return False


def is_image_exist(connection, biz_id, img_size):
    if biz_id is False:
        return False
    if img_size is False:
        return False

    select_sql = "SELECT id FROM photo_urls WHERE business_id = %s AND original_size = %s"
    rows = execute_fetch(connection, select_sql, [biz_id, img_size])
    if len(rows) > 0:
        return False  # если какие то строки есть, значит есть фотка, значит возвращаем фолс
    elif len(rows) <= 0:
        return True  # когда 0 или вообще записи нет, тогда возвращаем тру, значит можем сохранять эту фотку


def get_photo_size(connection, biz_id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT original_size FROM photo_urls WHERE business_id = %s"
    rows = execute_fetch(connection, sql_query, [biz_id])
    if len(rows) > 0:
        img_size = rows[0][0]
        return img_size
    else:
        return False


def add_more_info(connection, biz_id, about, SITE_ID):
    if connection is False:
        return False
    if biz_id is False:
        return False
    if about is False:
        return False

    values = (about, biz_id, SITE_ID)

    sql_query = f"""UPDATE businesses_info SET more_info = %s WHERE business_id = %s AND 
    site_id = %s"""
    update_more_info = execute_query(connection, sql_query, values)
    if update_more_info is not False:
        return True
    if update_more_info is False:
        return False


def add_review(connection, reviews, biz_id, SITE_ID):
    if biz_id is False:
        return False
    if connection is False:
        return False
    if reviews is False:
        return False

    for review in reviews:
        values = (biz_id, review['author'], review['date'], review['text'].decode(), review['rating'], SITE_ID)

        if is_exists_review(connection, review, biz_id, SITE_ID):

            sql_insert = f"""INSERT INTO reviews(business_id, author, date, text, rating, site_id)
            VALUES(%s, %s, %s, %s, %s, %s)"""
            execute_query(connection, sql_insert, values)

    return False


def is_exists_review(connection, review, biz_id, SITE_ID):
    if connection is False:
        return False
    if review is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id, review['author'], review['date'], SITE_ID)

    sql_query = f"""SELECT id  
    FROM reviews 
    WHERE business_id = %s
    AND author = %s
    AND date = %s 
    AND site_id = %s"""

    rows = execute_fetch_prepared(connection, sql_query, values)

    if len(rows) <= 0:
        return True
    if len(rows) > 0:
        return False


def select_count(connection, biz_id, SITE_ID):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id, SITE_ID)

    sql_query = "SELECT count FROM businesses_info WHERE business_id = %s AND site_id = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        rows = rows[0][0]
        return rows
    if len(rows) <= 0:
        return False


def update_count(connection, count, biz_id, SITE_ID):
    if connection is False:
        return False
    if biz_id is False:
        return False
    if count is False:
        return False

    values = (count, biz_id, SITE_ID)

    sql_query = "UPDATE businesses_info SET count = %s WHERE business_id = %s AND site_id = %s"
    return execute_query(connection, sql_query, values)


def select_SUM_rating_and_reviews_COUNT(connection, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id,)

    sql_query = """SELECT SUM(rating) as rating_summary, COUNT(*) as reviews_count  
                FROM reviews WHERE business_id = %s AND `rating` > 0"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        sum_rating_id_0 = rows[0][0]
        reviews_count_id_0 = rows[0][1]
        return sum_rating_id_0, reviews_count_id_0
    else:
        return False


def select_rating_count_id_9(connection, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id,)

    sql_query = """SELECT rating, count FROM businesses_info WHERE business_id = %s 
    AND site_id = 9 """
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        rating_site_id9 = rows[0][0]
        count_site_id9 = rows[0][1]
        return rating_site_id9, count_site_id9
    if len(rows) < 0:
        return False


def update_rating_biz(connection, rating, biz_id, SITE_ID):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (rating, biz_id, SITE_ID)

    sql_query = """UPDATE businesses_info SET rating = %s WHERE business_id = %s AND site_id = %s"""
    return execute_query(connection, sql_query, values)


def insert_site_id9(connection, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (SITE_ID_9, biz_id)

    sql_query = """INSERT INTO businesses_info (site_id, business_id) VALUES (%s, %s)"""
    return execute_query(connection, sql_query, values)


def if_exist_site_id9(connection, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id, )

    sql_query = """SELECT EXISTS(SELECT rating, count FROM businesses_info WHERE business_id = %s and site_id = 9)"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        rows = rows[0][0]
        rows = bool(rows)
        return rows


def select_site_id_0(connection, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id, 0)

    sql_query = """SELECT rating, count FROM businesses_info WHERE business_id = %s AND site_id = %s"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        rating_site_id_0 = rows[0][0]
        count_site_id_0 = rows[0][1]
        return rating_site_id_0, count_site_id_0
    if len(rows) < 0:
        return False


def update_rating_count_site_id_0(connection, rating, count, biz_id):
    if connection is False:
        return False
    if biz_id is False:
        return False
    if rating is False:
        return False
    if count is False:
        return False

    values = (rating, count, biz_id, 0)

    sql_query = """UPDATE businesses_info SET rating = %s, count = %s WHERE business_id = %s AND site_id = %s"""
    return execute_query(connection, sql_query, values)


def update_price_range(connection, biz_id, price_range, SITE_ID):
    if connection is False:
        return False
    if biz_id is False:
        return False
    if price_range is False:
        return False

    values = (price_range, biz_id, SITE_ID)

    sql_query = """UPDATE businesses_info SET price_range = %s WHERE business_id = %s AND site_id = %s"""
    return execute_query(connection, sql_query, values)


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


def insert_coupon(connection, brand_id, coupon, SITE_ID):
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

