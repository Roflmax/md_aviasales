
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select airline
from "aviasales"."public_dm"."dm_best_prices_per_flight"
where airline is null



  
  
      
    ) dbt_internal_test