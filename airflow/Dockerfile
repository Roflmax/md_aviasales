FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pymongo \
    psycopg2-binary \
    sqlalchemy \
    loguru \
    dbt-core \
    dbt-postgres \
    elementary-data
