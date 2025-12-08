-- Создание схемы STG (staging layer)
CREATE SCHEMA IF NOT EXISTS stg;

-- Таблица для хранения сырых данных (EL подход) с поддержкой SCD Type 2
-- Данные хранятся как JSONB без трансформации
CREATE TABLE IF NOT EXISTS stg.flight_prices_raw (
    id SERIAL PRIMARY KEY,
    -- Уникальный идентификатор билета из MongoDB
    flight_price_id VARCHAR(255) NOT NULL,
    raw_data JSONB NOT NULL,
    -- Время загрузки в MongoDB (для инкрементальной загрузки)
    fetched_at TIMESTAMP WITH TIME ZONE,
    -- Время загрузки в PostgreSQL
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- SCD Type 2 поля
    valid_from_dttm TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    valid_to_dttm TIMESTAMP WITH TIME ZONE DEFAULT '5999-12-31'::TIMESTAMP WITH TIME ZONE
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
-- Индекс для SCD Type 2 поиска актуальных записей
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_valid_to
    ON stg.flight_prices_raw (valid_to_dttm);
-- Индекс для поиска по flight_price_id
CREATE INDEX IF NOT EXISTS idx_flight_prices_raw_flight_id
    ON stg.flight_prices_raw (flight_price_id);
