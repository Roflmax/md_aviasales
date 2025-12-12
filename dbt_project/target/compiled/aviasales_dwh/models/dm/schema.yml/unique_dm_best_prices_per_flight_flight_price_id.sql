
    
    

select
    flight_price_id as unique_field,
    count(*) as n_records

from "aviasales"."public_dm"."dm_best_prices_per_flight"
where flight_price_id is not null
group by flight_price_id
having count(*) > 1


