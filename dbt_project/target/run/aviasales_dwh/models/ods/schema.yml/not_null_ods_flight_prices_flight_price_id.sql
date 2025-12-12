
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flight_price_id
from "aviasales"."public_ods"."ods_flight_prices"
where flight_price_id is null



  
  
      
    ) dbt_internal_test