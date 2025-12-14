{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='days_until_departure',
        tags=['dm', 'analytics']
    )
}}

/*
    Витрина: За сколько дней до вылета лучше покупать билет?

    Методология:
    - Для каждого рейса (departure_at + airline + flight_number) вычисляем среднюю цену
    - Считаем отклонение каждого наблюдения от средней цены ЭТОГО ЖЕ рейса
    - Группируем по days_until_departure
    - Отрицательное отклонение = в этот период цены НИЖЕ средней

    Идентификатор рейса: departure_at + airline + flight_number

    Инкрементальная загрузка:
    - Стратегия: merge (обновление существующих + добавление новых строк)
    - Unique key: days_until_departure
    - При инкрементальном запуске: пересчитываем только дни с новыми наблюдениями
*/

with flight_prices as (
    select
        days_until_departure,
        price,
        departure_at,
        airline,
        flight_number,
        loaded_at,
        -- Средняя цена этого конкретного рейса
        avg(price) over (
            partition by departure_at, airline, flight_number
        ) as flight_avg_price
    from {{ ref('ods_flight_prices') }}
    where days_until_departure >= 0
      and days_until_departure <= 90

    {% if is_incremental() %}
        -- При инкрементальном запуске берём только новые наблюдения
        and loaded_at > (select coalesce(max(last_observation), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

with_deviation as (
    select
        days_until_departure,
        price,
        flight_avg_price,
        price - flight_avg_price as price_deviation,
        loaded_at
    from flight_prices
)

select
    days_until_departure,

    -- Ключевая метрика: отклонение от средней цены рейса
    round(avg(price_deviation), 0) as avg_price_deviation,

    -- Контекст
    round(avg(price), 0) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    count(*) as observations_count,

    -- Рейтинг (1 = лучший период для покупки)
    rank() over (order by avg(price_deviation)) as price_rank,

    -- Метка времени последнего наблюдения (для инкрементальной загрузки)
    max(loaded_at) as last_observation

from with_deviation
group by days_until_departure
order by days_until_departure
