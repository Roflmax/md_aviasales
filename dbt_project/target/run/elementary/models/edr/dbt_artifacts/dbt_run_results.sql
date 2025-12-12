
      
        
        
        delete from "aviasales"."public"."dbt_run_results" as DBT_INTERNAL_DEST
        where (model_execution_id) in (
            select distinct model_execution_id
            from "dbt_run_results__dbt_tmp142652373191" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_run_results" ("model_execution_id", "unique_id", "invocation_id", "generated_at", "created_at", "name", "message", "status", "resource_type", "execution_time", "execute_started_at", "execute_completed_at", "compile_started_at", "compile_completed_at", "rows_affected", "full_refresh", "compiled_code", "failures", "query_id", "thread_id", "materialization", "adapter_response")
    (
        select "model_execution_id", "unique_id", "invocation_id", "generated_at", "created_at", "name", "message", "status", "resource_type", "execution_time", "execute_started_at", "execute_completed_at", "compile_started_at", "compile_completed_at", "rows_affected", "full_refresh", "compiled_code", "failures", "query_id", "thread_id", "materialization", "adapter_response"
        from "dbt_run_results__dbt_tmp142652373191"
    )
  