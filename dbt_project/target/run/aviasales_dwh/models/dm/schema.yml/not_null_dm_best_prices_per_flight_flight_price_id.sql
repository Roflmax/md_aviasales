
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flight_price_id
from "aviasales"."public_dm"."dm_best_prices_per_flight"
where flight_price_id is null



  
  
      
    ) dbt_internal_test