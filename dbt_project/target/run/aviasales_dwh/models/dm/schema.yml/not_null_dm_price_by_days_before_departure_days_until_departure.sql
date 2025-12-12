
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select days_until_departure
from "aviasales"."public_dm"."dm_price_by_days_before_departure"
where days_until_departure is null



  
  
      
    ) dbt_internal_test