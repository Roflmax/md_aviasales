from datetime import datetime
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB


def get_collection():
    """Возвращает коллекцию flight_prices."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db["flight_prices"]


def save_prices(prices: list) -> int:
    """Сохраняет список билетов в MongoDB. Возвращает количество."""
    if not prices:
        return 0

    collection = get_collection()

    # Добавляем метку времени к каждому билету
    for price in prices:
        price["fetched_at"] = datetime.utcnow()

    result = collection.insert_many(prices)
    return len(result.inserted_ids)


def get_prices(limit: int = 100) -> list:
    """Возвращает список билетов из базы."""
    collection = get_collection()

    documents = list(collection.find().limit(limit))

    # Преобразуем _id в строку для JSON-сериализации
    for doc in documents:
        doc["_id"] = str(doc["_id"])

    return documents
