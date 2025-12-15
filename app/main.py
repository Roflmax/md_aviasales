from api import fetch_prices
from db import get_prices, save_prices
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


# Модель запроса
class FetchRequest(BaseModel):
    origin: str
    destination: str
    departure_at: str = None


@app.get("/health")
def health():
    """Проверка работоспособности."""
    return {"status": "ok"}


@app.post("/fetch-prices")
def fetch_and_save(request: FetchRequest):
    """
    Загружает цены из Aviasales API и сохраняет в MongoDB.
    """
    prices = fetch_prices(request.origin, request.destination, request.departure_at)

    if prices:
        count = save_prices(prices)
        return {"success": True, "count": count}

    return {"success": False, "count": 0}


@app.get("/prices")
def list_prices(limit: int = 100):
    """
    Возвращает сохранённые билеты.
    """
    prices = get_prices(limit)
    return {"success": True, "data": prices}
