-- Создание схемы STG (staging layer)
CREATE SCHEMA IF NOT EXISTS stg;

-- Таблица для хранения сырых данных (EL подход, append-only лог)
-- Данные хранятся как JSONB без трансформации
CREATE TABLE IF NOT EXISTS stg.flight_prices_raw (
    id SERIAL PRIMARY KEY,
    -- Уникальный идентификатор записи из MongoDB
    flight_price_id VARCHAR(255) NOT NULL,
    raw_data JSONB NOT NULL,
    -- Время загрузки в MongoDB (для инкрементальной загрузки)
    fetched_at TIMESTAMP WITH TIME ZONE,
    -- Время загрузки в PostgreSQL
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для поиска по полям внутри JSON
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_origin
    ON stg.flight_prices_raw ((raw_data->>'origin'));
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_destination
    ON stg.flight_prices_raw ((raw_data->>'destination'));
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_loaded
    ON stg.flight_prices_raw (loaded_at);
-- Индекс для инкрементальной загрузки
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_fetched
    ON stg.flight_prices_raw (fetched_at);
