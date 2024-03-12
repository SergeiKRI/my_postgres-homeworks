"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os.path

import psycopg2


def unit():
    conn = psycopg2.connect(database='north',user='postgres', password='8987')
    # подключение к БД

    try:
        with conn:
            with conn.cursor() as cur:
                with open(os.path.join('north_data', 'employees_data.csv'), newline='') as employs:
                    employ = csv.DictReader(employs)
                    for e in employ:
                        cur.execute("INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)", (
                            e["employee_id"],
                            e["first_name"],
                            e["last_name"],
                            e["title"],
                            e["birth_date"],
                            e["notes"]
                        ))
                    print('таблица employees заполнена')

                with open(os.path.join('north_data', 'customers_data.csv'), newline='') as customers:
                    customer = csv.DictReader(customers)
                    for c in customer:
                        cur.execute("INSERT INTO customers VALUES(%s, %s, %s)", (
                            c["customer_id"],
                            c["company_name"],
                            c["contact_name"]
                        ))
                    print('таблица customers заполнена')

                with open(os.path.join('north_data', 'orders_data.csv'), newline='') as orders:
                    order = csv.DictReader(orders)
                    for i in order:
                        cur.execute("INSERT INTO orders VALUES(%s, %s, %s, %s, %s)", (
                            i["order_id"],
                            i["customer_id"],
                            i["employee_id"],
                            i["order_date"],
                            i["ship_city"]
                        ))
                    print('таблица orders заполнена')

    finally:
        conn.close()


if __name__ == '__main__':
    unit()