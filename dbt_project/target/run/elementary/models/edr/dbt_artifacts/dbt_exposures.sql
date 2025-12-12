
      
        
        
        delete from "aviasales"."public"."dbt_exposures" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_exposures__dbt_tmp142651594412" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_exposures" ("unique_id", "name", "maturity", "type", "owner_email", "owner_name", "url", "depends_on_macros", "depends_on_nodes", "depends_on_columns", "description", "tags", "meta", "package_name", "original_path", "path", "generated_at", "metadata_hash", "label", "raw_queries")
    (
        select "unique_id", "name", "maturity", "type", "owner_email", "owner_name", "url", "depends_on_macros", "depends_on_nodes", "depends_on_columns", "description", "tags", "meta", "package_name", "original_path", "path", "generated_at", "metadata_hash", "label", "raw_queries"
        from "dbt_exposures__dbt_tmp142651594412"
    )
  