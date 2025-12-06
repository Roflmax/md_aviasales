import os

# Aviasales API
API_TOKEN = os.getenv("AVIASALES_API_TOKEN")
API_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"

# MongoDB
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_DB = "aviasales"

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017"
