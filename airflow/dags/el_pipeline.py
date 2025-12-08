import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger
from pymongo import MongoClient
from sqlalchemy import Column, Integer, String, DateTime, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func


# Настройки из переменных окружения
def get_mongo_uri():
    user = os.getenv('MONGO_USER', 'admin')
    password = os.getenv('MONGO_PASSWORD', 'password123')
    host = os.getenv('MONGO_HOST', 'mongodb')
    return f"mongodb://{user}:{password}@{host}:27017"


def get_postgres_url():
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "aviasales")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


MONGO_DB = os.getenv("MONGO_DB", "aviasales")

# SQLAlchemy ORM
Base = declarative_base()


class FlightPriceRaw(Base):
    """Модель для хранения сырых данных о ценах (append-only лог)."""
    __tablename__ = "flight_prices_raw"
    __table_args__ = {"schema": "stg"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    flight_price_id = Column(String(255), nullable=False)
    raw_data = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime(timezone=True))
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())


def ensure_schema_exists(engine):
    """Создание схемы stg если не существует."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))


def get_last_fetched_at(session, shift_hours: int = 1) -> datetime:
    """
    Получение времени последней загрузки из PostgreSQL.

    :param session: SQLAlchemy сессия
    :param shift_hours: Сдвиг в часах для перестраховки
    :return: Datetime для фильтрации MongoDB
    """
    last_fetched = session.query(func.max(FlightPriceRaw.fetched_at)).scalar()

    if last_fetched is None:
        logger.info("No previous data found, will load all records")
        return datetime(1970, 1, 1)

    # Сдвигаем назад для перестраховки
    result = last_fetched - timedelta(hours=shift_hours)
    logger.info(f"Last fetched_at: {last_fetched}, loading from: {result}")
    return result


def get_data_from_mongo(fetched_after: datetime = None):
    """
    Инкрементальная загрузка данных из MongoDB.

    :param fetched_after: Загружать только записи новее этой даты
    :return: Generator с документами
    """
    client = None
    try:
        client = MongoClient(get_mongo_uri())
        db = client[MONGO_DB]
        collection = db["flight_prices"]

        # Инкрементальная загрузка - только новые записи
        if fetched_after:
            filter_condition = {"fetched_at": {"$gt": fetched_after}}
            logger.info(f"Fetching documents with fetched_at > {fetched_after}")
        else:
            filter_condition = {}
            logger.info("Fetching all documents (initial load)")

        cursor = collection.find(filter_condition)
        count = 0

        for doc in cursor:
            count += 1
            yield doc

        logger.info(f"Fetched {count} documents from MongoDB")

    except Exception as e:
        logger.error(f"Error fetching from MongoDB: {e}")
        raise
    finally:
        if client:
            client.close()


def insert_flight_prices(session, documents):
    """
    Загрузка данных в PostgreSQL (простой INSERT, append-only лог).
    """
    inserted_count = 0

    for doc in documents:
        try:
            flight_price_id = str(doc["_id"])

            # Подготавливаем данные для вставки
            doc_copy = dict(doc)
            doc_copy["_id"] = str(doc_copy["_id"])

            # Преобразуем datetime объекты
            fetched_at = doc.get("fetched_at")
            if fetched_at and hasattr(fetched_at, "isoformat"):
                doc_copy["fetched_at"] = fetched_at.isoformat()

            # Создаём новую запись
            new_record = FlightPriceRaw(
                flight_price_id=flight_price_id,
                raw_data=doc_copy,
                fetched_at=fetched_at,
            )
            session.add(new_record)
            inserted_count += 1

        except Exception as e:
            logger.error(f"Error processing document {doc.get('_id')}: {e}")
            raise

    session.commit()
    logger.info(f"Inserted {inserted_count} records")
    return inserted_count


def extract_and_load(**context):
    """
    Основная функция EL пайплайна.
    Извлекает данные из MongoDB и загружает в PostgreSQL.
    """
    engine = create_engine(get_postgres_url())
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Убеждаемся что схема существует
        ensure_schema_exists(engine)

        # Создаём таблицу если не существует
        Base.metadata.create_all(engine, checkfirst=True)

        # Получаем время последней загрузки
        last_fetched = get_last_fetched_at(session)

        # Извлекаем новые данные из MongoDB
        documents = get_data_from_mongo(fetched_after=last_fetched)

        # Загружаем в PostgreSQL
        count = insert_flight_prices(session, documents)

        logger.info(f"EL pipeline completed successfully. Processed {count} records.")
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"EL pipeline failed: {e}")
        raise
    finally:
        session.close()


# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "el_flight_prices",
    default_args=default_args,
    description="Extract flight prices from MongoDB, Load to PostgreSQL (incremental)",
    schedule_interval="0 * * * *",  # каждый час
    catchup=False,
    tags=["el", "aviasales", "incremental"],
) as dag:

    el_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )
