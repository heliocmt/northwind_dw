{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_country') }}
    ),
    transformed as (
        select
        row_number() over (order by ship_country) as country_sk
        , ship_country
        , order_id
        from staging
    )
    select * from transformed