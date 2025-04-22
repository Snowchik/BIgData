import random
import datetime
import psycopg2
from faker import Faker

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "user": "forum_user",
    "password": "forum_pass",
    "dbname": "forum_logs"
}

fake = Faker()
NUM_USERS = 20
START_DATE = datetime.date.today().replace(day=1)
DAYS_IN_MONTH = 30

ACTION_TYPES = [
    "site_entry", "register", "login", "logout",
    "create_topic", "visit_topic", "delete_topic",
    "write_message"
]

def get_connection():
    return psycopg2.connect(**DB_PARAMS)

def create_users(conn):
    users = []
    with conn.cursor() as cur:
        for _ in range(NUM_USERS):
            username = fake.user_name()
            email = fake.email()
            cur.execute(
                "INSERT INTO users (username, email) VALUES (%s, %s) RETURNING id",
                (username, email)
            )
            users.append(cur.fetchone()[0])
    conn.commit()
    return users

def insert_log(conn, user_id, action_type, entity_id, entity_type, status, description, date):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO logs (user_id, action_type, entity_id, entity_type, status, description, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, action_type, entity_id, entity_type, status, description,
             datetime.datetime.combine(date, fake.time_object()))
        )

def insert_topic(conn, user_id, title, date):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO topics (user_id, title, created_at) VALUES (%s, %s, %s) RETURNING id",
            (user_id, title, datetime.datetime.combine(date, fake.time_object()))
        )
        return cur.fetchone()[0]

def insert_message(conn, user_id, topic_id, content, is_anon, date):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO messages (user_id, topic_id, content, is_anonymous, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (user_id, topic_id, content, is_anon, datetime.datetime.combine(date, fake.time_object()))
        )
        return cur.fetchone()[0]

def generate_day_logs(conn, users, date):
    num_anon = 0
    num_logged = 0
    topic_ids = []

    for _ in range(5):
        insert_log(conn, None, "site_entry", None, None, "success", "User entered the site", date)

        uid = random.choice(users)
        insert_log(conn, uid, "register", uid, "user", "success", "User registered", date)

        uid = random.choice(users)
        insert_log(conn, uid, "login", None, None, "success", "User logged in", date)
        insert_log(conn, uid, "logout", None, None, "success", "User logged out", date)

        if random.random() < 0.4:
            insert_log(conn, None, "create_topic", None, "topic", "error", "User not logged in", date)
        else:
            uid = random.choice(users)
            topic_id = insert_topic(conn, uid, fake.sentence(), date)
            topic_ids.append(topic_id)
            insert_log(conn, uid, "create_topic", topic_id, "topic", "success", "Topic created", date)

        if topic_ids:
            topic_id = random.choice(topic_ids)
            insert_log(conn, random.choice(users), "visit_topic", topic_id, "topic", "success", "Topic viewed", date)

        if topic_ids and random.random() < 0.3:
            topic_id = random.choice(topic_ids)
            insert_log(conn, random.choice(users), "delete_topic", topic_id, "topic", "success", "Topic deleted", date)

        if topic_ids:
            topic_id = random.choice(topic_ids)
            is_anon = random.choice([True, False])
            if is_anon:
                msg_id = insert_message(conn, None, topic_id, fake.text(), True, date)
                insert_log(conn, None, "write_message", msg_id, "message", "success", "Anon message", date)
                num_anon += 1
            else:
                uid = random.choice(users)
                msg_id = insert_message(conn, uid, topic_id, fake.text(), False, date)
                insert_log(conn, uid, "write_message", msg_id, "message", "success", "User message", date)
                num_logged += 1

def main():
    conn = get_connection()
    users = create_users(conn)

    for i in range(DAYS_IN_MONTH):
        day = START_DATE + datetime.timedelta(days=i)
        generate_day_logs(conn, users, day)

    conn.commit()
    conn.close()
    print("Данные сгенерированы и загружены.")

if __name__ == "__main__":
    main()
