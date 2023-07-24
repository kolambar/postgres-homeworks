import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    #create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                print("Данные получены")
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, suppliers)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'CREATE DATABASE {db_name}')

    cur.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file) as file:
        data = file.read()
        cur.execute(data)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
                CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id SERIAL PRIMARY KEY,
                company_name VARCHAR,
                contact VARCHAR NOT NULL,
                address VARCHAR NOT NULL,
                phone VARCHAR(25),
                fax VARCHAR(25),
                homepage VARCHAR
                )
                """)

def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file) as file:
        data = json.load(file)
        return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    # print(repr(suppliers))
    for sup in suppliers:
        #print(sup["company_name"], sup["contact"], sup["address"], sup["phone"], sup["fax"], sup["homepage"])
        #print(type(sup["company_name"]), type(sup["contact"]), type(sup["address"]), type(sup["phone"]), type(sup["fax"]), type(sup["homepage"]))
        cur.execute(
            """
            INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING supplier_id
            """,
            (sup["company_name"], sup["contact"], sup["address"], sup["phone"], sup["fax"], sup["homepage"])
        )
        supplier_id = cur.fetchone()[0]
        sup['supplier_id'] = supplier_id

def add_foreign_keys(cur, suppliers) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("""
                ALTER TABLE products ADD COLUMN supplier_id INT;
                ALTER TABLE products ADD CONSTRAINT fk_products_supplier_id FOREIGN KEY(supplier_id) 
                REFERENCES suppliers (supplier_id)
                """
                )
    for sup in suppliers:
        cur.execute(
            """
            UPDATE products
            SET supplier_id = %s
            WHERE product_name in %s 
            """,
            (sup['supplier_id'], tuple(sup['products']))
        )

if __name__ == '__main__':
    main()

'''
            INSERT INTO products (supplier_id)
            WHERE product_name in %s
            VALUE (%s)
            RETURNING product_name
'''

"""
            SELECT supplier_id FROM products
            WHERE product_name in %s
            INSERT INTO products (supplier_id)
            VALUE (%s)
"""