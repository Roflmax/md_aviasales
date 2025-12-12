
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observations_count
from "aviasales"."public_dm"."dm_price_by_days_before_departure"
where observations_count is null



  
  
      
    ) dbt_internal_test