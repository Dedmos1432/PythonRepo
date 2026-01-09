from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import sql


KAFKA_TOPIC = "db_topic"
KAFKA_SERVERS = "localhost:9092"
KAFKA_GROUP = "pg_writer_group"

POSTGRES_CFG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


def init_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )


def open_db():
    connection = psycopg2.connect(**POSTGRES_CFG)
    connection.autocommit = False
    return connection, connection.cursor()


def ensure_table(cursor, connection, table, fields):
    cols = ", ".join(f"{name} TEXT" for name in fields)

    query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ({})"
    ).format(
        sql.Identifier(table),
        sql.SQL(cols)
    )

    cursor.execute(query)
    connection.commit()


def write_rows(cursor, connection, table, rows):
    if not rows:
        return

    fields = list(rows[0].keys())

    stmt = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({})"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, fields)),
        sql.SQL(", ").join(sql.Placeholder() * len(fields))
    )

    for item in rows:
        cursor.execute(stmt, [item[f] for f in fields])

    connection.commit()


def main():
    consumer = init_consumer()
    conn, cur = open_db()

    print("Kafka → PostgreSQL consumer запущен")

    try:
        for record in consumer:
            try:
                payload = record.value
                tbl = payload.get("table")
                rows = payload.get("data")

                if not rows:
                    print("пропуск")
                    continue

                ensure_table(cur, conn, tbl, rows[0].keys())
                write_rows(cur, conn, tbl, rows)

                print(f"Записано строк: {len(rows)} → {tbl}")

            except Exception as err:
                print("Ошибка обработки:", err)
                conn.rollback()

    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
