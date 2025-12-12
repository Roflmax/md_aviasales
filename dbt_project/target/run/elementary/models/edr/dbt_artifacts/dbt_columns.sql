
      
        
        
        delete from "aviasales"."public"."dbt_columns" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_columns__dbt_tmp142651622001" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_columns" ("unique_id", "parent_unique_id", "name", "data_type", "tags", "meta", "database_name", "schema_name", "table_name", "description", "resource_type", "generated_at", "metadata_hash")
    (
        select "unique_id", "parent_unique_id", "name", "data_type", "tags", "meta", "database_name", "schema_name", "table_name", "description", "resource_type", "generated_at", "metadata_hash"
        from "dbt_columns__dbt_tmp142651622001"
    )
  