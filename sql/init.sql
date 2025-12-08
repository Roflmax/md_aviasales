-- Создание схемы STG (staging layer)
CREATE SCHEMA IF NOT EXISTS stg;

-- Таблица для хранения сырых данных (EL подход)
-- Данные хранятся как JSONB без трансформации
CREATE TABLE IF NOT EXISTS stg.flight_prices_raw (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для поиска по полям внутри JSON
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_origin
    ON stg.flight_prices_raw ((raw_data->>'origin'));
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_destination
    ON stg.flight_prices_raw ((raw_data->>'destination'));
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_loaded
    ON stg.flight_prices_raw (loaded_at);
