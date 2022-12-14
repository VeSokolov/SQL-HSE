import psycopg2
import psycopg2 as pg_driver
from psycopg2 import Error
from psycopg2.extras import NamedTupleCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Connection to PostgreSQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query, fetch_result=False):
    cursor = connection.cursor(cursor_factory=NamedTupleCursor)
    cursor.execute(query)
    connection.commit()
    if fetch_result:
        return cursor.fetchall()


def execute_queries(db, sql_commands):
    db.autocommit = True
    with db.cursor() as cursor:
        for sql_command in sql_commands:
            cursor.execute(sql_command)


def create_database(database_name):
    try:
        connection = psycopg2.connect(user="postgres", password="94Q2%WRJ61", host="127.0.0.1", port="5432")
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        rows = cursor.fetchall()
        try:
            cursor.execute(f"DROP database {database_name};")
        except (Exception, Error) as error: pass
        sql_create_database = f"create database {database_name};"
        create_user = [
                f"CREATE USER testuser with encrypted password 'testuser';",
                f"GRANT ALL PRIVILEGES ON DATABASE {database_name} TO testuser;"]
        cursor.execute(sql_create_database)
        execute_queries(database_name, create_user)
    except (Exception, Error) as error:
        print("Ошибка ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение закрыто")


# добавление инструмента во все связанные с ним таблицы --- ГОТОВО, НО НАДО ПРОТЕСТИРОВАТЬ
def add_instrument(instr_type, material, manufacturer, price, location, status, **kwargs):
    max_id = execute_query(db, "SELECT max(id) FROM instrument")
    match instr_type:
        case "guitar":
            sql_command = f"""INSERT INTO guitar (id, string_num, fret_num, floyd_rose, shape)
                            VALUES
                            ({max_id}, {kwargs.string_num}, {kwargs.fret_num}, {kwargs.floyd_rose}, 
                            SELECT shape_id FROM shape WHERE shape_name = {kwargs.shape})"""
            execute_query(db, sql_command)
            instr_type = [1, "guitar"]
            pass
        case "keyboard":
            sql_command = f"""INSERT INTO keyboard (id, keynum, is_synth)
                            VALUES
                            ({max_id}, {kwargs.keynum}, {kwargs.is_synth})"""
            execute_query(db, sql_command)
            instr_type = [2, "keyboard"]
            pass
        case "drum":
            instr_type = [3, "drum"]
            sql_command = f"""INSERT INTO guitar (id, diameter)
                            VALUES
                            ({max_id}, {kwargs.diameter})"""
            execute_query(db, sql_command)
            pass
    sql_command = f"""INSERT INTO instrument (nstr_type, material, location, status, price)
                VALUES
                    ({instr_type[0]},
                    SELECT material_id FROM material WHERE material_name = {material},
                    SELECT location_id FROM location WHERE location_name = {location},
                    SELECT status_name FROM status WHERE status_name = {status},
                    {price})"""
    execute_query(db, sql_command)
    set_manufacturer_info = f"""INSERT INTO instrument_manufacturer (instrument_id, manufacturer_id)
                            VALUES
                            ({max_id}, 
                            SELECT manufacturer_id from manufacturer WHERE manufacturer_name = {manufacturer})"""
    execute_query(db, set_manufacturer_info)
    pass


# создание заказа (с соответствующим изменением таблиц инструментов) --- НЕ ГОТОВО
def create_order():
    order = f"""INSERT INTO oder_history (order_id, date, location_id, cashier_id)
                VALUES
                (SELECT max(id) from )"""
    execute_query(db, order)
    execute_query(db, "UPDATE instrument SET status = 'in delivery'")
    pass


# завершение заказа (проданный инструмент удаляется из БД) --- НЕ ГОТОВО
def finish_order(order_id):
    pass


# написание собственных запросов (в 1 строку) --- НЕ ГОТОВО
def user_sql_command():
    sql_command = input()
    execute_query(db, sql_command)


# создание чистой БД
database_name = "shopdb"

db = create_connection(database_name, "postgres", "94Q2%WRJ61", "localhost", "5432")

# создаём ВСЕ таблицы --- ГОТОВО, НАДО ПРОТЕСТИРОВАТЬ
creating_tables = [
                """DROP TABLE IF EXISTS instruments""",
                """DROP TABLE IF EXISTS status""",
                """DROP TABLE IF EXISTS location""",
                """DROP TABLE IF EXISTS material""",
                """DROP TABLE IF EXISTS guitar""",
                """DROP TABLE IF EXISTS keyboard""",
                """DROP TABLE IF EXISTS drum""",
                """DROP TABLE IF EXISTS shape""",
                """DROP TABLE IF EXISTS manufacturer""",
                """DROP TABLE IF EXISTS employees""",
                """DROP TABLE IF EXISTS order_history""",
                """DROP TABLE IF EXISTS orders_instruments""",

                """CREATE TABLE instruments(
                    id SERIAL PRIMARY KEY,
                    instr_type INT NOT NULL,
                    manufacturer INT REFERENCES manufacturer(manufacturer_id),
                    material INT REFERENCES material(material_id),
                    location INT REFERENCES location(location_id),
                    status INT REFERENCES status(status_id),
                    price INT NOT NULL
                );
                """,
                """CREATE TABLE status(
                    status_id INT NOT NULL,
                    status_name TEXT NOT NULL
                );
                """,
                """CREATE TABLE location(
                    location_id INT NOT NULL,
                    address VARCHAR NOT NULL
                );
                """,
                """CREATE TABLE material(
                    material_id INT NOT NULL,
                    material_name TEXT NOT NULL
                );
                """,
                """CREATE TABLE guitar(
                    id INT REFERENCES instrument(id),
                    string_num INT NOT NULL,
                    fret_num INT NOT NULL,
                    floyd_rose BIT NOT NULL,
                    shape INT REFERENCES shape(shape_id)
                );
                """,
                """CREATE TABLE keyboard(
                    id INT REFERENCES instrument(id),
                    keynum INT NOT NULL,
                    is_synth BIT
                );
                """,
                """CREATE TABLE drum(
                    id INT REFERENCES instrument(id),
                    diameter INT NOT NULL
                );
                """,
                """CREATE TABLE shape(
                    shape_id INT NOT NULL,
                    shape_name TEXT NOT NULL
                );
                """,
                """CREATE TABLE manufacturer(
                    manufacturer_id INT NOT NULL,
                    manufacturer_name VARCHAR NOT NULL
                );
                """,
                """CREATE TABLE orders_instruments(
                    order_id INT REFERENCES order_history(order_id),
                    instrument_id INT REFERENCES instrument(id)
                );""",
                """CREATE TABLE order_history(
                    order_id INT NOT NULL,
                    date TIMESTAMP,
                    location_id INT NOT NULL,
                    cashier_id INT NOT NULL
                );
                """,
                """CREATE TABLE employees(
                    employee_id INT NOT NULL,
                    employee_name TEXT NOT NULL,
                );
                """]
execute_queries(db, creating_tables)

# заполняем таблицы, которые не будут меняться по мере работы --- ГОТОВО, НАДО ПРОТЕСТИРОВАТЬ
filling_static_tables = [
    """ INSERT INTO location (location_id, address)
    VALUES
        (1, 'Moscow'),
        (2, 'St. Petersburg'),
        (3, 'Kazan')
        """,
    """INSERT INTO status (status_id, status_name)
    VALUES
        (1, 'in stock'),
        (2, 'in shop'),
        (3, 'in delivery')
        """,
    """INSERT INTO material (material_id, material_name)
        VALUES
            (1, 'plastic'),
            (2, 'wood'),
            (3, 'carbon'),
            (4, 'metal')
            """,
    """INSERT INTO manufacturer (manufacturer_id, manufacturer_name)
        VALUES
            (1, 'Gibson'),
            (2, 'Yamaha'),
            (3, 'Fender'),
            (4, 'Remo')
            """,
    """INSERT INTO shape (shape_id, shape_name)
        VALUES
            (1, 'Telecaster'),
            (2, 'Stratocaster'),
            (3, 'Explorer'),
            (4, 'Flying V')
            """,
    """INSERT INTO employees (employee_id, employee_name)
        VALUES
            (1, 'Mark'),
            (2, 'Egor'),
            (3, 'Irina')
            """]
execute_queries(db, filling_static_tables)

# заполняем таблицы, изменяемые функциями add_instrument, create_order и finish_order --- ГОТОВО, НАДО ПРОТЕСТИРОВАТЬ
filling_dynamic_tables = [
    """INSERT INTO instruments (instr_type, manufacturer, material, location, status, price)
        VALUES
            (1, 1, 2, 1, 1, 10000),
            (1, 2, 2, 2, 1, 15000),
            (1, 2, 2, 2, 2, 15500),
            (1, 2, 2, 3, 1, 23000),
            (1, 1, 2, 1, 1, 50000),
            (2, 3, 1, 1, 1, 6000),
            (2, 3, 2, 2, 2, 80000),
            (2, 3, 1, 1, 1, 11000),
            (2, 2, 1, 3, 2, 12990),
            (3, 4, 2, 3, 2, 8000),
            (3, 4, 3, 1, 2, 4000),
            (3, 4, 3, 1, 2, 4500)
    """,
    """INSERT INTO guitar (id, strung_num, fret_num, floyd_rose, shape)
        VALUES
            (1, 6, 21, 0, 1),
            (2, 6, 21, 0, 2),
            (3, 6, 22, 1, 1),
            (4, 6, 21, 1, 3),
            (5, 7, 22, 0, 2)
    """,
    """INSERT INTO keyboard (id, keynum, is_synth)
        VALUES
            (6, 52, 1),
            (7, 108, 0),
            (8, 64, 1),
            (9, 64, 0)
    """,
    """INSERT INTO drum (id, diameter)
            VALUES
                (10, 32),
                (11, 30),
                (12, 30)
        """]
execute_queries(db, filling_dynamic_tables)
