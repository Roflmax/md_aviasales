import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import psycopg2


# Настройки из переменных окружения
MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb://{os.getenv('MONGO_USER', 'admin')}:{os.getenv('MONGO_PASSWORD', 'password123')}@mongodb:27017"
)
MONGO_DB = os.getenv("MONGO_DB", "aviasales")

POSTGRES_CONN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "aviasales"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_from_mongo(**context):
    """Извлечение сырых данных из MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["flight_prices"]

    documents = list(collection.find())

    # Преобразуем ObjectId в строку для JSON-сериализации
    for doc in documents:
        doc["_id"] = str(doc["_id"])
        # Преобразуем datetime объекты в ISO строки
        if "fetched_at" in doc and hasattr(doc["fetched_at"], "isoformat"):
            doc["fetched_at"] = doc["fetched_at"].isoformat()

    client.close()

    # Сохраняем в XCom
    context["ti"].xcom_push(key="raw_data", value=documents)

    return len(documents)


def load_to_postgres(**context):
    """Загрузка сырых JSON данных в PostgreSQL (EL подход)."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_from_mongo", key="raw_data")

    if not raw_data:
        return 0

    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    count = 0
    for doc in raw_data:
        # Сохраняем весь документ как JSONB без трансформации
        cursor.execute(
            "INSERT INTO stg.flight_prices_raw (raw_data) VALUES (%s)",
            (json.dumps(doc),)
        )
        count += 1

    conn.commit()
    cursor.close()
    conn.close()

    return count


with DAG(
    "el_flight_prices",
    default_args=default_args,
    description="Extract raw flight prices from MongoDB, Load to PostgreSQL as JSONB",
    schedule_interval="*/1 * * * *",  # каждую минуту
    catchup=False,
    tags=["el", "aviasales"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_mongo",
        python_callable=extract_from_mongo,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract_task >> load_task
