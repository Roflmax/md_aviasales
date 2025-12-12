
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select record_id
from "aviasales"."public_ods"."ods_flight_prices"
where record_id is null



  
  
      
    ) dbt_internal_test