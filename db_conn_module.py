# https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursorprepared.html - prepare query
# https://proglib.io/p/kak-podruzhit-python-i-bazy-dannyh-sql-podrobnoe-rukovodstvo-2020-02-27 - work with mysql
# https://pynative.com/python-mysql-execute-parameterized-query-using-prepared-statement/ tuple values in execute query

import mysql.connector
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        pass
    except Error as e:
        print(e)
        pass

    return connection


def execute_query_without_value(connection, query):  # for create db
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute(query)  # need tuple values
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        cursor.close()
        return False


def execute_query(connection, query, value=None):
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute(query, value)  # need tuple values
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        cursor.close()
        print(e)
        return False


def execute_fetch_prepared(connection, query, value=None):
    cursor = connection.cursor(prepared=True)
    result = None
    try:
        cursor.execute(query, value)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Error as e:
        cursor.close()
        print(e)
        pass


def execute_fetch(connection, query, value=None):
    cursor = connection.cursor(buffered=True)
    result = None
    try:
        cursor.execute(query, value)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Error as e:
        cursor.close()
        print(e)
        return False


def execute_fetch_one(connection, query, value=None):
    cursor = connection.cursor(buffered=True)
    result = None
    try:
        cursor.execute(query, value)
        result = cursor.fetchone()
        cursor.close()
        return result
    except Error as e:
        cursor.close()
        print(e)
        pass
