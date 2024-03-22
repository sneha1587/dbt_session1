{{
    config( materialized='table')
}}
with src_reviews as(
    select * from {{ref('src_reviews')}}
)
select * from src_reviews
where review_text is not null