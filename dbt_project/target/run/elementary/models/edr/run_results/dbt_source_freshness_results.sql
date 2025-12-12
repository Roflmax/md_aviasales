
      
        
        
        delete from "aviasales"."public"."dbt_source_freshness_results" as DBT_INTERNAL_DEST
        where (source_freshness_execution_id) in (
            select distinct source_freshness_execution_id
            from "dbt_source_freshness_results__dbt_tmp142652984377" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_source_freshness_results" ("source_freshness_execution_id", "unique_id", "max_loaded_at", "snapshotted_at", "generated_at", "created_at", "max_loaded_at_time_ago_in_s", "status", "error", "compile_started_at", "compile_completed_at", "execute_started_at", "execute_completed_at", "invocation_id", "warn_after", "error_after", "filter")
    (
        select "source_freshness_execution_id", "unique_id", "max_loaded_at", "snapshotted_at", "generated_at", "created_at", "max_loaded_at_time_ago_in_s", "status", "error", "compile_started_at", "compile_completed_at", "execute_started_at", "execute_completed_at", "invocation_id", "warn_after", "error_after", "filter"
        from "dbt_source_freshness_results__dbt_tmp142652984377"
    )
  