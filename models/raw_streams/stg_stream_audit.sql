{{
    config(
        materialized='incremental',
        unique_key=['table_name', 'row_id', 'primary_key', 'data_hash'],
        on_schema_change='append_new_columns',
        full_refresh=false
    )
}}

WITH combine_streams AS (
    SELECT 'STREAM_TABLE1' AS table_name,
        metadata$row_id AS row_id,
        metadata$isupdate AS is_update,
        metadata$action AS action_type,
        primary_key AS primary_key,
        SHA2(column1 || column2, 256) AS data_hash,
        OBJECT_CONSTRUCT('primary_key', primary_key, 'column1', column1, 'column2', column2) AS column_json
    FROM {{ source('raw_streams', 'stream1') }}

    UNION

    SELECT 'STREAM_TABLE2' AS table_name,
        metadata$row_id AS row_id,
        metadata$isupdate AS is_update,
        metadata$action AS action_type,
        primary_key AS primary_key,
        SHA2(column1 || column2, 256) AS data_hash,
        OBJECT_CONSTRUCT('primary_key', primary_key, 'column1', column1, 'column2', column2) AS column_json
    FROM {{ source('raw_streams', 'stream2') }}
)

SELECT table_name,
       row_id,
       is_update,
       action_type,
       primary_key,
       data_hash,
       column_json
  FROM combine_streams