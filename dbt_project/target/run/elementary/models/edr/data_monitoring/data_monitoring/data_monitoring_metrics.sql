
      
        
        
        delete from "aviasales"."public"."data_monitoring_metrics" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "data_monitoring_metrics__dbt_tmp142651748915" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."data_monitoring_metrics" ("id", "full_table_name", "column_name", "metric_name", "metric_type", "metric_value", "source_value", "bucket_start", "bucket_end", "bucket_duration_hours", "updated_at", "dimension", "dimension_value", "metric_properties", "created_at")
    (
        select "id", "full_table_name", "column_name", "metric_name", "metric_type", "metric_value", "source_value", "bucket_start", "bucket_end", "bucket_duration_hours", "updated_at", "dimension", "dimension_value", "metric_properties", "created_at"
        from "data_monitoring_metrics__dbt_tmp142651748915"
    )
  