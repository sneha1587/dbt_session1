{% snapshot type2scd_snapshot %}
{{
    config(
        target_database='airbnb',
        target_schema='dbt_dev',
        unique_key='host_id',
       strategy='timestamp',
        updated_at='updated_at',
    )
}}
/***first iteration will create snapshot table**/
/*WITH raw_hosts AS (
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
    raw_hosts */
/**for second iteration pull incremental records***/
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
    where (updated_at > (SELECT MAX(UPDATED_AT) FROM {{source('dim_layer','dim_hosts_type2')}})
    or host_id not in (select host_id from {{source('dim_layer','dim_hosts_type2')}})
    )


{% endsnapshot %}