

/*
    Витрина: Лучшие цены на каждый рейс

    Бизнес-вопрос: Какая минимальная цена была зафиксирована на рейс?
    Используется оконная функция row_number() для выбора лучшего предложения
*/

with prices_ranked as (
    select
        flight_price_id,
        airline,
        flight_number,
        departure_at,
        price,
        fetched_at,
        days_until_departure,

        -- Ранжируем по цене внутри каждого рейса (1 = самая низкая цена)
        row_number() over (
            partition by flight_price_id
            order by price asc, fetched_at desc
        ) as price_rank,

        -- Сколько раз мы видели этот рейс
        count(*) over (partition by flight_price_id) as times_seen,

        -- Статистика цен на этот рейс
        min(price) over (partition by flight_price_id) as min_price_ever,
        max(price) over (partition by flight_price_id) as max_price_ever,
        round(avg(price) over (partition by flight_price_id), 0) as avg_price

    from "aviasales"."public_ods"."ods_flight_prices"
),

best_prices as (
    select
        flight_price_id,
        airline,
        flight_number,
        departure_at,

        -- Лучшая цена и когда она была
        price as best_price,
        fetched_at as best_price_found_at,
        days_until_departure as days_before_at_best_price,

        -- Контекст
        times_seen,
        min_price_ever,
        max_price_ever,
        avg_price,

        -- Разница между худшей и лучшей ценой
        max_price_ever - min_price_ever as price_spread,

        -- Экономия от покупки по лучшей цене vs средней
        round(avg_price - min_price_ever, 0) as potential_savings

    from prices_ranked
    where price_rank = 1  -- только лучшая цена на рейс
)

select *
from best_prices
order by departure_at, airline