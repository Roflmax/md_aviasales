{{
    config(
        materialized='table',
        tags=['dm', 'analytics']
    )
}}

/*
    Витрина: В какое время лучше искать билеты?

    Методология:
    - Для каждого рейса (departure_at + airline + flight_number) вычисляем среднюю цену
    - Считаем отклонение каждого наблюдения от средней цены ЭТОГО ЖЕ рейса
    - Группируем по дню недели и часу парсинга
    - Отрицательное отклонение = в это время цены НИЖЕ средней

    Использование:
    - Напрямую: 168 строк (7 дней × 24 часа)
    - Агрегация по дню: GROUP BY fetch_day_of_week
    - Агрегация по часу: GROUP BY fetch_hour

    Идентификатор рейса: departure_at + airline + flight_number
*/

with flight_prices as (
    select
        fetch_day_of_week,
        fetch_hour,
        price,
        departure_at,
        airline,
        flight_number,
        -- Средняя цена этого конкретного рейса
        avg(price) over (
            partition by departure_at, airline, flight_number
        ) as flight_avg_price
    from {{ ref('ods_flight_prices') }}
),

with_deviation as (
    select
        fetch_day_of_week,
        fetch_hour,
        price,
        flight_avg_price,
        price - flight_avg_price as price_deviation
    from flight_prices
)

select
    fetch_day_of_week,
    fetch_hour,

    -- Название дня недели
    case fetch_day_of_week
        when 0 then 'Воскресенье'
        when 1 then 'Понедельник'
        when 2 then 'Вторник'
        when 3 then 'Среда'
        when 4 then 'Четверг'
        when 5 then 'Пятница'
        when 6 then 'Суббота'
    end as day_name,

    -- Ключевая метрика: отклонение от средней цены рейса
    round(avg(price_deviation), 0) as avg_price_deviation,

    -- Контекст
    round(avg(price), 0) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    count(*) as observations_count,

    -- Рейтинг (1 = лучшее время для поиска)
    rank() over (order by avg(price_deviation)) as price_rank

from with_deviation
group by fetch_day_of_week, fetch_hour
order by fetch_day_of_week, fetch_hour
