
    
        
        

        
        
        
        

        
        
        
        

        
        


        

        
        
            
        
            
        
            
        
            
        
            
        
            
        
            
        
            
        
            
        

        
        
        

        

        
        
        

        

        
        
        

        
        
        


        
        
        
        

        select * from (with anomaly_scores as (
        select
            id,
            metric_id,
            test_execution_id,
            test_unique_id,
            detected_at,
            full_table_name,
            column_name,
            metric_name,
            anomaly_score,
            anomaly_score_threshold,
            anomalous_value,
            bucket_start,
            bucket_end,
            bucket_seasonality,
            metric_value,
            min_metric_value,
            max_metric_value,
            training_avg,
            training_stddev,
            training_set_size,
            training_start,
            training_end,
            dimension,
            dimension_value,
            
    case
        when dimension is not null and column_name is null then 
    'The last ' || metric_name || ' value for dimension ' || dimension || ' - ' ||
    case when dimension_value is null then 'NULL' else dimension_value end || ' is ' || cast(round(cast(metric_value as numeric(28,6)), 3) as varchar(4096)) ||
    '. The average for this metric is ' || cast(round(cast(training_avg as numeric(28,6)), 3) as varchar(4096)) || '.'

        when dimension is not null and column_name is not null then 
    'In column ' || column_name || ', the last ' || metric_name || ' value for dimension ' || dimension || ' is ' || cast(round(cast(metric_value as numeric(28,6)), 3) as varchar(4096)) ||
    '. The average for this metric is ' || cast(round(cast(training_avg as numeric(28,6)), 3) as varchar(4096)) || '.'

        when metric_name = 'freshness' then 
    'Last update was at ' || anomalous_value || ', ' || cast(abs(round(cast(metric_value/3600 as numeric(28,6)), 2)) as varchar(4096)) || ' hours ago. Usually the table is updated within ' || cast(abs(round(cast(training_avg/3600 as numeric(28,6)), 2)) as varchar(4096)) || ' hours.'

        when column_name is null then 
    'The last ' || metric_name || ' value is ' || cast(round(cast(metric_value as numeric(28,6)), 3) as varchar(4096)) ||
    '. The average for this metric is ' || cast(round(cast(training_avg as numeric(28,6)), 3) as varchar(4096)) || '.'

        when column_name is not null then 
    'In column ' || column_name || ', the last ' || metric_name || ' value is ' || cast(round(cast(metric_value as numeric(28,6)), 3) as varchar(4096)) ||
    '. The average for this metric is ' || cast(round(cast(training_avg as numeric(28,6)), 3) as varchar(4096)) || '.'

        else null
    end as anomaly_description
,
            max(bucket_end) over (partition by test_execution_id) as max_bucket_end
        from "aviasales"."public"."test_6fbad481e1_eleme__anomaly_scores__tmp_20251212110245183824"
      ),
      anomaly_scores_with_is_anomalous as (
        select
          *,
case when
          (
            
  (anomaly_score is not null and
  (
    
  (
    metric_value = 0 and 
    
      1 = 2
    
  )
 or
    (
      case when metric_name IN 
        ( 'freshness' ,  'event_freshness'  )
 then
            anomaly_score > 3
    else
        
        abs(anomaly_score) > 3
    

     end and
      (
  
  
  
  
    
        (1 = 1)
    

    and

    
        (1 = 1)
    
  
  )
    )
  ))

          )
          and bucket_end > 
    cast(max_bucket_end as timestamp) + cast('-2' as integer) * INTERVAL '1 day'

          then TRUE else FALSE end as is_anomalous
        from anomaly_scores
      ),

      final_results as (
          select
          metric_value as value,
          training_avg as average,
          
          case
          when is_anomalous = TRUE and 'both' = 'spike' then
          lag(metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
          when is_anomalous = TRUE and 'both' != 'spike' then
          lag(min_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
          when 'both' = 'spike' then metric_value
          else min_metric_value end as min_value,
          case
          when is_anomalous = TRUE and 'both' = 'drop' then
          lag(metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
          when is_anomalous = TRUE and 'both' != 'drop' then
          lag(max_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
          when 'both' = 'drop' then metric_value
          else max_metric_value end as max_value,
          bucket_start as start_time,
          bucket_end as end_time,
          *
        from anomaly_scores_with_is_anomalous
        order by bucket_end, dimension_value
      )

      select * from final_results
      where 1 = 1) results
    where is_anomalous = true
