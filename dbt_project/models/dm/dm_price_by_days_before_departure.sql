{{
    config(
        materialized='table',
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
*/

with flight_prices as (
    select
        days_until_departure,
        price,
        departure_at,
        airline,
        flight_number,
        -- Средняя цена этого конкретного рейса
        avg(price) over (
            partition by departure_at, airline, flight_number
        ) as flight_avg_price
    from {{ ref('ods_flight_prices') }}
    where days_until_departure >= 0
      and days_until_departure <= 90
),

with_deviation as (
    select
        days_until_departure,
        price,
        flight_avg_price,
        price - flight_avg_price as price_deviation
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
    rank() over (order by avg(price_deviation)) as price_rank

from with_deviation
group by days_until_departure
order by days_until_departure
