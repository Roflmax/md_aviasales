
      
        
        
        delete from "aviasales"."public"."dbt_sources" as DBT_INTERNAL_DEST
        where (unique_id) in (
            select distinct unique_id
            from "dbt_sources__dbt_tmp142653167111" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."dbt_sources" ("unique_id", "database_name", "schema_name", "source_name", "name", "identifier", "loaded_at_field", "freshness_warn_after", "freshness_error_after", "freshness_filter", "freshness_description", "relation_name", "tags", "meta", "owner", "package_name", "original_path", "path", "source_description", "description", "generated_at", "metadata_hash")
    (
        select "unique_id", "database_name", "schema_name", "source_name", "name", "identifier", "loaded_at_field", "freshness_warn_after", "freshness_error_after", "freshness_filter", "freshness_description", "relation_name", "tags", "meta", "owner", "package_name", "original_path", "path", "source_description", "description", "generated_at", "metadata_hash"
        from "dbt_sources__dbt_tmp142653167111"
    )
  