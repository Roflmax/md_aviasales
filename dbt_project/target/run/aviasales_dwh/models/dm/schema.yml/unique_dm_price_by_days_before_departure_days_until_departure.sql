
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    days_until_departure as unique_field,
    count(*) as n_records

from "aviasales"."public_dm"."dm_price_by_days_before_departure"
where days_until_departure is not null
group by days_until_departure
having count(*) > 1



  
  
      
    ) dbt_internal_test