
      
        
        
        delete from "aviasales"."public"."dbt_models" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_models__dbt_tmp142652358902" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_models" ("unique_id", "alias", "checksum", "materialization", "tags", "meta", "owner", "database_name", "schema_name", "depends_on_macros", "depends_on_nodes", "description", "name", "package_name", "original_path", "path", "patch_path", "generated_at", "metadata_hash")
    (
        select "unique_id", "alias", "checksum", "materialization", "tags", "meta", "owner", "database_name", "schema_name", "depends_on_macros", "depends_on_nodes", "description", "name", "package_name", "original_path", "path", "patch_path", "generated_at", "metadata_hash"
        from "dbt_models__dbt_tmp142652358902"
    )
  