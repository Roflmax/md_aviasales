
      
        
        
        delete from "aviasales"."public"."dbt_seeds" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_seeds__dbt_tmp142652412605" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_seeds" ("unique_id", "alias", "checksum", "tags", "meta", "owner", "database_name", "schema_name", "description", "name", "package_name", "original_path", "path", "generated_at", "metadata_hash")
    (
        select "unique_id", "alias", "checksum", "tags", "meta", "owner", "database_name", "schema_name", "description", "name", "package_name", "original_path", "path", "generated_at", "metadata_hash"
        from "dbt_seeds__dbt_tmp142652412605"
    )
  