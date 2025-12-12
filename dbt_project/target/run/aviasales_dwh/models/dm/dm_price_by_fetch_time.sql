
  
    

  create  table "aviasales"."public_dm"."dm_price_by_fetch_time__dbt_tmp"
  
  
    as
  
  (
    

/*
    Витрина: Средняя цена по времени парсинга

    Бизнес-вопрос: В какой день недели и час лучше искать билеты?
    Гипотеза: Цены могут меняться в зависимости от времени суток и дня недели
*/

with prices as (
    select
        fetch_day_of_week,
        fetch_hour,
        price
    from "aviasales"."public_ods"."ods_flight_prices"
)

select
    fetch_day_of_week,
    fetch_hour,

    -- Название дня недели для читаемости
    case fetch_day_of_week
        when 0 then 'Воскресенье'
        when 1 then 'Понедельник'
        when 2 then 'Вторник'
        when 3 then 'Среда'
        when 4 then 'Четверг'
        when 5 then 'Пятница'
        when 6 then 'Суббота'
    end as day_name,

    -- Агрегаты по цене
    round(avg(price), 0) as avg_price,
    min(price) as min_price,
    max(price) as max_price,

    -- Количество наблюдений
    count(*) as observations_count,

    -- Рейтинг (1 = лучшее время для покупки)
    rank() over (order by avg(price)) as price_rank

from prices
group by fetch_day_of_week, fetch_hour
order by fetch_day_of_week, fetch_hour
  );
  