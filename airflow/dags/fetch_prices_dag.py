from datetime import datetime, timedelta

import requests
from airflow.operators.python import PythonOperator
from loguru import logger

from airflow import DAG

HOST = "app"  # имя сервиса в docker-compose


def task_fetch_prices():
    """
    Функция для получения цен на авиабилеты из Aviasales API
    """
    json_data = {
        "origin": "LED",
        "destination": "SVX",
    }

    try:
        logger.info(
            f"Fetching prices for {json_data['origin']} -> {json_data['destination']}"
        )
        response = requests.post(
            f"http://{HOST}:8000/fetch-prices", json=json_data, timeout=30
        )
        response.raise_for_status()

        result = response.json()
        count = result.get("count", 0)
        logger.info(f"Successfully fetched {count} prices")

        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch prices: {e}")
        raise


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_flight_prices",
    default_args=default_args,
    description="Fetch flight prices from Aviasales API",
    schedule_interval="0 * * * *",  # каждый час
    catchup=False,
    tags=["aviasales", "fetch"],
)

with dag:
    fetch_task = PythonOperator(
        task_id="fetch_prices",
        python_callable=task_fetch_prices,
    )

    _ = fetch_task
