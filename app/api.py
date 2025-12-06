import httpx
from config import API_TOKEN, API_URL


def fetch_prices(origin: str, destination: str, departure_at: str = None) -> list:
    """
    Запрашивает цены из Aviasales API.

    Возвращает список билетов или пустой список при ошибке.
    """
    params = {
        "origin": origin,
        "destination": destination,
        "token": API_TOKEN,
        "currency": "rub",
    }

    if departure_at:
        params["departure_at"] = departure_at

    try:
        response = httpx.get(API_URL, params=params)
        data = response.json()

        if data.get("success"):
            return data.get("data", [])
        return []

    except Exception:
        return []
