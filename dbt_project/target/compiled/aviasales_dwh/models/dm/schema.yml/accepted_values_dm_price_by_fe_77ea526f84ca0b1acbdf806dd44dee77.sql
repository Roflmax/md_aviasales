
    
    

with all_values as (

    select
        fetch_day_of_week as value_field,
        count(*) as n_records

    from "aviasales"."public_dm"."dm_price_by_fetch_time"
    group by fetch_day_of_week

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)


