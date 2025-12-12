
    
    

with all_values as (

    select
        airline as value_field,
        count(*) as n_records

    from "aviasales"."public_ods"."ods_flight_prices"
    group by airline

)

select *
from all_values
where value_field not in (
    'DP','U6','5N','SU','S7'
)


