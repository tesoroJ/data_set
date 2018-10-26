from mimesis import Business, Address
import psycopg2
import random

create_table_customer = "CREATE table test_customer (" \
                        "id SERIAL PRIMARY KEY ," \
                        "name VARCHAR(70) UNIQUE)"

create_table_branch = "CREATE TABLE branch (" \
                      "id SERIAL PRIMARY KEY , " \
                      "street VARCHAR(80), " \
                      "city VARCHAR(40), " \
                      "state VARCHAR(40), " \
                      "zip VARCHAR(5), " \
                      "customer_id INTEGER NOT NULL , " \
                      "FOREIGN KEY (customer_id) REFERENCES test_customer (id), " \
                      "UNIQUE (address, customer_id))"

quarie="select * from test_customer"

INSERT = "INSERT INTO test_customer(name) SELECT $${}$$ " \
         "WHERE NOT EXISTS ( " \
         "SELECT name FROM test_customer WHERE name=$${}$$)"

# INSERT_ADDRESS ="INSERT INTO branch(address, customer_id) SELECT $${}$$, $${}$$" \
#                 "WHERE NOT EXISTS ( " \
#                 "SELECT customer_id FROM branch WHERE customer_id=$${}%%) "

INSERT_ADDRESS = "INSERT INTO branch(address, customer_id) VALUES ($${}$$, $${}$$)"


def db_connection(databname, login, password, host):
    """Функция для подключения к базе данных"""
    conn = psycopg2.connect(dbname=databname,host=host, user=login, password=password)
    cur = conn.cursor()
    return cur, conn


def sql_quaries(sql, curs):
    """Выполнение SQL запросов"""
    curs.execute(sql)
    # return curs.fetchall()


def sql_insert(sql, curs):
    curs.execute(sql)


def main():
    cursor, co = db_connection('fee_test', 'postgres', 'password', host='localhost')
    sql_quaries(create_table_customer, cursor)
    sql_quaries(create_table_branch, cursor)
    # print(create_table_branch)
    for i in range(1, 21):
        b = Business().company()
        sql_insert(INSERT.format(b, b), cursor)
        co.commit()
    for i in range(100):
        a = Address('en').address() + " " + Address('en').zip_code() + " " + Address('en').state()
        sql_insert(INSERT_ADDRESS.format(a, random.randint(1, 20)), cursor)
        co.commit()


if __name__ == '__main__':
    main()
