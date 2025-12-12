
      
        
        
        delete from "aviasales"."public"."dbt_metrics" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_metrics__dbt_tmp142652315470" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_metrics" ("unique_id", "name", "label", "model", "type", "sql", "timestamp", "filters", "time_grains", "dimensions", "depends_on_macros", "depends_on_nodes", "description", "tags", "meta", "package_name", "original_path", "path", "generated_at", "metadata_hash")
    (
        select "unique_id", "name", "label", "model", "type", "sql", "timestamp", "filters", "time_grains", "dimensions", "depends_on_macros", "depends_on_nodes", "description", "tags", "meta", "package_name", "original_path", "path", "generated_at", "metadata_hash"
        from "dbt_metrics__dbt_tmp142652315470"
    )
  