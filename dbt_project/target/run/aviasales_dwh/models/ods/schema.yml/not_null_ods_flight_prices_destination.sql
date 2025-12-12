
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select destination
from "aviasales"."public_ods"."ods_flight_prices"
where destination is null



  
  
      
    ) dbt_internal_test