{{
 config(
 materialized = 'incremental',
 on_schema_change='fail'
 )
}}
WITH src_reviews AS (
 SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

-----------------------------
--DBT RUN the model
--------------------------------
--add the below code in fct_reviews.sql at the end

{% if is_incremental() %}
 AND review_date > (select max(review_date) from {{ this }})
{% endif %}