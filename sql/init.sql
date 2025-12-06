-- Создание схемы STG (staging layer)
CREATE SCHEMA IF NOT EXISTS stg;

-- Таблица для хранения цен на билеты
CREATE TABLE IF NOT EXISTS stg.flight_prices (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3) NOT NULL,
    destination VARCHAR(3) NOT NULL,
    departure_at TIMESTAMP WITH TIME ZONE,
    return_at TIMESTAMP WITH TIME ZONE,
    price DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'RUB',
    airline VARCHAR(2),
    flight_number VARCHAR(10),
    transfers INTEGER DEFAULT 0,
    duration INTEGER,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_flight_prices_route ON stg.flight_prices(origin, destination);
CREATE INDEX IF NOT EXISTS idx_flight_prices_departure ON stg.flight_prices(departure_at);
CREATE INDEX IF NOT EXISTS idx_flight_prices_fetched ON stg.flight_prices(fetched_at);
