import psycopg2 as pg_driver
from psycopg2 import Error
from psycopg2.extras import NamedTupleCursor


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = pg_driver.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
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


def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")    # НИЧЕГО НЕ ДЕЛАЕТ


def execute_queries(db, sql_commands):
    db.autocommit = True
    with db.cursor() as cursor:
        for sql_command in sql_commands:
            cursor.execute(sql_command)


def add_like(author_id, entity, entity_id, time, receiver):
    query = f"INSERT INTO likes (user_id_send, entity_liked, created, user_id_receive) VALUES " \
            f"({author_id}, {entity}, {entity_id}, {time}, {receiver})"
    execute_query(db, query, fetch_result=True)


def add_photo(author_id, photo_id, photo_name):
    query = f"INSERT INTO photos (photo_id, author_id, photo_name) VALUES" \
            f"({photo_id}, {author_id}, {photo_name})"
    execute_query(db, query, fetch_result=True)


def add_comment(author_id, post_id, comment_text):
    query = f"INSERT INTO comments (post_id, comment_author_id, comment_text) VALUES" \
            f"({post_id}, {author_id}, {comment_text})"
    execute_query(db, query, fetch_result=True)


def recall_like(entity_id):
    query = f"DELETE FROM likes WHERE entity_id = {entity_id}"
    # удаление происходит ТОЛЬКО по IDшнику, т.к. в задании не уточнено, как именно нужно
    execute_query(db, query, fetch_result=True)


db = create_connection("testdb", "postgres", "94Q2%WRJ61", "localhost", "5432")

cur = db.cursor()


#------------------------

creating_database = [
                "CREATE USER testuser with encrypted password 'testuser';",
                "GRANT ALL PRIVILEGES ON DATABASE testdb TO testuser;"]
#  execute_queries(db, creating_database)
creating_tables = [
                "DROP TABLE IF EXISTS users;",
                "DROP TABLE IF EXISTS likes;",
                "DROP TABLE IF EXISTS photos;",
                "DROP TABLE IF EXISTS comments;",
                """CREATE TABLE users (
                         user_id INT NOT NULL,
                         name TEXT NOT NULL,
                         created TIMESTAMP NOT NULL
                );
                """,
                """CREATE TABLE likes (
                         user_id_send INT NOT NULL,
                         entity_liked TEXT NOT NULL,
                         entity_id INT NOT NULL, 
                         created TIMESTAMP NOT NULL,
                         user_id_receive INT NOT NULL
                );
                """,  # возможные типы сущностей: PERSON, PHOTO, COMMENT
                """CREATE TABLE photos(
                         photo_id INT NOT NULL, 
                         author_id INT NOT NULL,
                         photo_name VARCHAR NOT NULL
                );
                """,
                """CREATE TABLE comments(
                             post_id INT NOT NULL, 
                             comment_author_id INT NOT NULL,
                             comment_text VARCHAR NOT NULL
                );
                """]  # Создали таблицы юзеров и лайков
execute_queries(db, creating_tables)

add_users = """ INSERT INTO users (user_id, created, name) 
            VALUES 
                 (1, to_timestamp('16-05-2021 15:36:38', 'dd-mm-yyyy hh24:mi:ss'), 'Mark'),
                 (2, to_timestamp('16-06-2018 14:36:38', 'dd-mm-yyyy hh24:mi:ss'), 'Dima'),
                 (3, to_timestamp('16-07-2020 09:36:38', 'dd-mm-yyyy hh24:mi:ss'), 'Phill'),
                 (4, to_timestamp('16-06-2018 15:36:38', 'dd-mm-yyyy hh24:mi:ss'), 'Viktor'),
                 (5, to_timestamp('16-06-2018 16:36:38', 'dd-mm-yyyy hh24:mi:ss'), 'Ivan')
        """  # добавили юзеров
execute_query(db, add_users)

add_likes = """ INSERT INTO likes (user_id_send, entity_liked, entity_id, created, user_id_receive) 
            VALUES 
                 (1, 'PERSON', 4, to_timestamp('16-05-2021 15:36:38', 'dd-mm-yyyy hh24:mi:ss'), 4),
                 (1, 'PERSON', 5, to_timestamp('16-06-2018 14:36:38', 'dd-mm-yyyy hh24:mi:ss'), 5),
                 (2, 'PERSON', 5, to_timestamp('16-06-2018 14:36:38', 'dd-mm-yyyy hh24:mi:ss'), 5)
        """  # добавили лайки в формате (id отправителя, что было лайкнуто, id лайкнутого объекта, время, id получателя)
execute_query(db, add_likes)


test_request = "select user_id, " \
               "name, " \
               "sum(case when likes.user_id_send = users.user_id then 1 else 0 end) as likes_given, " \
               "sum(case when likes.user_id_receive = users.user_id then 1 else 0 end) as likes_received, " \
               "sum(case when likes.user_id_receive = users.user_id then 1 else 0 end) as likes_mutual " \
               "from users, likes " \
               "group by name, user_id"  # Взаимные лайки ещё не реализованы и потому отображаются неправильно

executed_request = execute_query(db, test_request, fetch_result=True)


test_request = "select user_id, " \
               "name, " \
               "sum(case when likes.user_id_receive = users.user_id then 1 else 0 end) as likes_received " \
               "from users, likes " \
               "group by name, user_id " \
               "ORDER BY likes_received DESC LIMIT 5"  # ВЫВОД 5 САМЫХ ПОПУЛЯРНЫХ
executed_request = execute_query(db, test_request, fetch_result=True)
