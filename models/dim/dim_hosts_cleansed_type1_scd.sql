
{{
  config(
    materialized = 'incremental',
    incremental_strategy='delete+insert',
    pre_hook='delete from AIRBNB.DBT_DEV.DIM_HOSTS_CLEANSED_TYPE1_SCD',
    unique_key='host_id'
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
