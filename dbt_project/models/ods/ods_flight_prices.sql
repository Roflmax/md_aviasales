{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='record_id',
        tags=['ods', 'flight_prices']
    )
}}

/*
    ODS модель: парсинг JSONB из STG слоя

    Что делает:
    - Извлекает поля из raw_data JSONB
    - Фильтрует только маршрут LED → SVX
    - Вычисляет days_until_departure (за сколько дней до вылета спарсили)
    - Инкрементально добавляет только новые записи
*/

with source as (
    select
        id as record_id,
        flight_price_id,
        raw_data,
        fetched_at,
        loaded_at
    from {{ source('stg', 'flight_prices_raw') }}

    {% if is_incremental() %}
        -- При инкрементальном запуске берём только новые записи
        where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

parsed as (
    select
        record_id,
        flight_price_id,

        -- Основные поля билета
        raw_data->>'origin' as origin,
        raw_data->>'destination' as destination,
        (raw_data->>'price')::integer as price,
        raw_data->>'airline' as airline,
        raw_data->>'flight_number' as flight_number,

        -- Даты и время
        (raw_data->>'departure_at')::timestamp as departure_at,
        (raw_data->>'duration')::integer as duration_minutes,
        (raw_data->>'transfers')::integer as transfers,

        -- Метаданные
        fetched_at,
        loaded_at,

        -- Вычисляемые поля для аналитики
        extract(dow from fetched_at) as fetch_day_of_week,  -- 0=Вс, 1=Пн, ..., 6=Сб
        extract(hour from fetched_at) as fetch_hour,

        -- Сколько дней до вылета был спарсен билет
        date_part('day',
            (raw_data->>'departure_at')::timestamp - fetched_at
        )::integer as days_until_departure

    from source
)

-- Финальный SELECT: только LED → SVX
select *
from parsed
where origin = 'LED'
  and destination = 'SVX'
  and price > 0
