"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv


PATH_TO_DB_DIR = os.path.join(os.path.abspath('..'), 'homework-1', 'north_data')

PATH_TO_CUS_DB = os.path.join(PATH_TO_DB_DIR, 'customers_data.csv')
PATH_TO_EMP_DB = os.path.join(PATH_TO_DB_DIR, 'employees_data.csv')
PATH_TO_ORD_DB = os.path.join(PATH_TO_DB_DIR, 'orders_data.csv')


#  Подключается к БД
conn = psycopg2.connect(database="north", user="postgres", password="6002")


def open_csv(path):
    """
    Функция для получения списка со списками для баз данных из csv файлов
    принимает путь до файла
    :return списка для БД:
    """

    with open(path, 'r', encoding='utf=8') as file:
        data = csv.reader(file)
        list_with_data = []

        for line in data:
            list_with_data.append(line)
        list_with_data.pop(0)

        return list_with_data


#  пробует внести данные в таблицы
try:
    with conn.cursor() as cur:

        #  вносит данные в таблицу customers
        customers_data = open_csv(PATH_TO_CUS_DB)
        for line in customers_data:
            cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', (line[0], line[1], line[2]))

        #  вносит данные в таблицу employees
        employees_data = open_csv(PATH_TO_EMP_DB)
        for line in employees_data:
            cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                       (line[0], line[1], line[2], line[3], line[4], line[5]))

        #  вносит данные в таблицу orders
        orders_data = open_csv(PATH_TO_ORD_DB)
        for line in orders_data:
            cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', (line[0], line[1], line[2], line[3], line[4]))

        #  делает коммит, если все хорошо
        conn.commit()
finally:
    conn.close()
