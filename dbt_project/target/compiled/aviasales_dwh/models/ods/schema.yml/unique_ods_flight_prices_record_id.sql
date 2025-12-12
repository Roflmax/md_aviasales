
    
    

select
    record_id as unique_field,
    count(*) as n_records

from "aviasales"."public_ods"."ods_flight_prices"
where record_id is not null
group by record_id
having count(*) > 1


