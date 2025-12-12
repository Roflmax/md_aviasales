
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select flight_price_id as from_field
    from "aviasales"."public_dm"."dm_best_prices_per_flight"
    where flight_price_id is not null
),

parent as (
    select flight_price_id as to_field
    from "aviasales"."public_ods"."ods_flight_prices"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test