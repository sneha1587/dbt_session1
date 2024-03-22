with src_hosts AS(
SELECT * FROM {{ref('src_host')}}

)
select 
host_id,
NVL(host_name,'Anonymous') as host_name,
is_superhost,
created_at,
updated_at
from src_host