from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2


# Настройки подключений
MONGO_URI = "mongodb://admin:password123@mongodb:27017"
MONGO_DB = "aviasales"

POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "database": "aviasales",
    "user": "airflow",
    "password": "airflow",
}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_from_mongo(**context):
    """Извлечение данных из MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["flight_prices"]

    documents = list(collection.find())

    # Преобразуем _id в строку
    for doc in documents:
        doc["_id"] = str(doc["_id"])

    # Сохраняем в XCom
    context["ti"].xcom_push(key="prices", value=documents)

    return len(documents)


def load_to_postgres(**context):
    """Загрузка данных в PostgreSQL."""
    ti = context["ti"]
    prices = ti.xcom_pull(task_ids="extract_from_mongo", key="prices")

    if not prices:
        return 0

    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    count = 0
    for price in prices:
        cursor.execute(
            """
            INSERT INTO stg.flight_prices (
                origin, destination, departure_at, return_at,
                price, currency, airline, flight_number,
                transfers, duration, fetched_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                price.get("origin"),
                price.get("destination"),
                price.get("departure_at"),
                price.get("return_at"),
                price.get("price"),
                price.get("currency", "RUB"),
                price.get("airline"),
                price.get("flight_number"),
                price.get("transfers", 0),
                price.get("duration_to") or price.get("duration"),
                price.get("fetched_at"),
            ),
        )
        count += 1

    conn.commit()
    cursor.close()
    conn.close()

    return count


with DAG(
    "el_flight_prices",
    default_args=default_args,
    description="Extract flight prices from MongoDB, Load to PostgreSQL",
    schedule_interval="@hourly",
    catchup=False,
    tags=["etl", "aviasales"],
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
