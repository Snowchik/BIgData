import psycopg2
import pandas as pd
import datetime
import sys

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "user": "forum_user",
    "password": "forum_pass",
    "dbname": "forum_logs"
}

def get_connection():
    return psycopg2.connect(**DB_PARAMS)

def aggregate_data(start_date: str, end_date: str, output_file: str = "aggregated_logs.csv"):
    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
    WITH dates AS (
        SELECT generate_series(
            DATE %s,
            DATE %s,
            INTERVAL '1 day'
        )::date AS day
    ),
    new_accounts AS (
        SELECT date(created_at) AS day, COUNT(*) AS registrations
        FROM users
        GROUP BY date(created_at)
    ),
    messages_by_day AS (
        SELECT date(created_at) AS day,
               COUNT(*) AS total_messages,
               SUM(CASE WHEN is_anonymous THEN 1 ELSE 0 END) AS anon_messages
        FROM messages
        GROUP BY date(created_at)
    ),
    topics_by_day AS (
        SELECT date(created_at) AS day,
               COUNT(*) AS new_topics
        FROM topics
        GROUP BY date(created_at)
    )
    SELECT
        d.day,
        COALESCE(na.registrations, 0) AS registrations,
        COALESCE(m.total_messages, 0) AS total_messages,
        ROUND(CASE
            WHEN m.total_messages > 0 THEN 100.0 * m.anon_messages / m.total_messages
            ELSE 0
        END, 2) AS anon_percentage,
        COALESCE(t.new_topics, 0) AS new_topics
    FROM dates d
    LEFT JOIN new_accounts na ON d.day = na.day
    LEFT JOIN messages_by_day m ON d.day = m.day
    LEFT JOIN topics_by_day t ON d.day = t.day
    ORDER BY d.day
    """

    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=[
        "day", "registrations", "total_messages", "anon_percentage", "new_topics"
    ])

    df["topic_growth_percent"] = df["new_topics"].pct_change().fillna(0) * 100
    df["topic_growth_percent"] = df["topic_growth_percent"].round(2)

    df.to_csv(output_file, index=False)
    print(f"Агрегированные данные сохранены в файл {output_file}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python aggregate_logs.py YYYY-MM-DD YYYY-MM-DD")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    aggregate_data(start_date, end_date)
