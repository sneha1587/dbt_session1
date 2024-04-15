{{
  config(
    materialized = 'incremental',
    unique_key='host_id',
    incremental_strategy='merge'
    
    
     )
}} 
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts_type1') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts