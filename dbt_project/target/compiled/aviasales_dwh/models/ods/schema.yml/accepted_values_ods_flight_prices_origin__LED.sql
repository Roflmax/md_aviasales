
    
    

with all_values as (

    select
        origin as value_field,
        count(*) as n_records

    from "aviasales"."public_ods"."ods_flight_prices"
    group by origin

)

select *
from all_values
where value_field not in (
    'LED'
)


