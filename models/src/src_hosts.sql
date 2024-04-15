/****Incremental pull from Raw tables***/
{{
    config(
        materialized = 'view',
        unique_key = 'host_id'
       
    )
}}

WITH raw_hosts AS (
    SELECT
        *
    FROM
        {{source('raw_layer','hosts')}}
)
SELECT
    id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts 
    where (updated_at > (SELECT MAX(UPDATED_AT) FROM {{source('dim_layer','dim_hosts')}})
    or host_id not in (select host_id from {{source('dim_layer','dim_hosts')}})
    )
