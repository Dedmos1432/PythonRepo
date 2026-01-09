from kafka import KafkaProducer
import json
import os

KAFKA_HOSTS = "localhost:9092"
KAFKA_TOPIC = "db_topic"


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_HOSTS,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8")
    )


def load_from_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def check_rows(rows):
    if not rows:
        return False, "Набор данных пуст"

    base = set(rows[0].keys())
    for r in rows:
        if set(r.keys()) != base:
            return False, "Обнаружено несовпадение полей в строках"

    return True, None


def publish(producer, topic, payload):
    result = producer.send(topic, value=payload)
    result.get(timeout=10)
    producer.flush()


def run():
    producer = build_producer()

    while True:
        mode = input("\njson / exit: ").strip().lower()

        if mode == "exit":
            print("Завершение работы отправителя")
            break

        if mode == "json":
            path = input("Введите путь к JSON-файлу: ").strip()
            if not os.path.isfile(path):
                print("Указанный файл не найден")
                continue

            table = input("Введите имя целевой таблицы: ").strip()
            rows = load_from_file(path)

            ok, error = check_rows(rows)
            if not ok:
                print("Ошибка проверки данных:", error)
                continue

            packet = {
                "table": table,
                "data": rows
            }

            try:
                publish(producer, KAFKA_TOPIC, packet)
                print(f"Передано записей: {len(rows)}. Таблица назначения: {table}")
            except Exception as exc:
                print("Сбой при отправке данных в Kafka:", exc)

        else:
            print("Выбран некорректный режим ввода")
