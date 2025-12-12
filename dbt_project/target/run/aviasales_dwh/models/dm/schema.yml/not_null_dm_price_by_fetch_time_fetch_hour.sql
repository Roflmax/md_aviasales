
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fetch_hour
from "aviasales"."public_dm"."dm_price_by_fetch_time"
where fetch_hour is null



  
  
      
    ) dbt_internal_test