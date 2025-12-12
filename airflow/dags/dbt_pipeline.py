"""
DAG для запуска dbt трансформаций.

Выполняет:
1. dbt run - создаёт/обновляет модели (ODS → DM)
2. dbt test - проверяет качество данных
3. elementary report - генерирует HTML отчёт
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Пути к dbt проекту
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
ELEMENTARY_REPORT_PATH = "/opt/airflow/reports/elementary_report.html"

# Общие аргументы для dbt команд
DBT_BASE_CMD = f"cd {DBT_PROJECT_DIR} && dbt"
DBT_ARGS = f"--profiles-dir {DBT_PROFILES_DIR}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 6),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_flight_prices",
    default_args=default_args,
    description="DBT трансформации: STG → ODS → DM витрины",
    schedule_interval="30 * * * *",  # каждый час в :30 (после EL пайплайна в :00)
    catchup=False,
    tags=["dbt", "aviasales", "transform"],
) as dag:

    # Задача 1: Установка зависимостей (пакетов)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE_CMD} deps {DBT_ARGS}",
    )

    # Задача 2: Запуск моделей (ODS + DM)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BASE_CMD} run {DBT_ARGS}",
    )

    # Задача 3: Запуск тестов (dbt-core + elementary)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BASE_CMD} test {DBT_ARGS}",
    )

    # Задача 4: Генерация Elementary отчёта
    elementary_report = BashOperator(
        task_id="elementary_report",
        bash_command=f"edr report --profiles-dir {DBT_PROFILES_DIR} --file-path {ELEMENTARY_REPORT_PATH} || true",
    )

    # Граф зависимостей
    # deps → run → test → report
    dbt_deps >> dbt_run >> dbt_test >> elementary_report
