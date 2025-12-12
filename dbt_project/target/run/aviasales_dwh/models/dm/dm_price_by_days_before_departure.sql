
  
    

  create  table "aviasales"."public_dm"."dm_price_by_days_before_departure__dbt_tmp"
  
  
    as
  
  (
    

/*
    Витрина: Средняя цена по дням до вылета

    Бизнес-вопрос: За сколько дней до вылета лучше покупать билет?
    Ответ: Найти days_until_departure с минимальной avg_price
*/

with prices as (
    select
        days_until_departure,
        price,
        airline
    from "aviasales"."public_ods"."ods_flight_prices"
    where days_until_departure >= 0  -- только будущие вылеты
      and days_until_departure <= 90 -- не дальше 3 месяцев
)

select
    days_until_departure,

    -- Агрегаты по цене
    round(avg(price), 0) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    round(stddev(price), 0) as price_stddev,

    -- Количество наблюдений
    count(*) as observations_count,
    count(distinct airline) as airlines_count,

    -- Рекомендация (чем ниже avg_price - тем лучше)
    rank() over (order by avg(price)) as price_rank

from prices
group by days_until_departure
order by days_until_departure
  );
  