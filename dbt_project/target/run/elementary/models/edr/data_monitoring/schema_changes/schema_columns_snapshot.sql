
      
        
        
        delete from "aviasales"."public"."schema_columns_snapshot" as DBT_INTERNAL_DEST
        where (column_state_id) in (
            select distinct column_state_id
            from "schema_columns_snapshot__dbt_tmp142653743536" as DBT_INTERNAL_SOURCE
        );

    

    insert into "aviasales"."public"."schema_columns_snapshot" ("column_state_id", "full_column_name", "full_table_name", "column_name", "data_type", "is_new", "detected_at", "created_at")
    (
        select "column_state_id", "full_column_name", "full_table_name", "column_name", "data_type", "is_new", "detected_at", "created_at"
        from "schema_columns_snapshot__dbt_tmp142653743536"
    )
  