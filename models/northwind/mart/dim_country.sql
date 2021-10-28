{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_country') }}
    ),
    transformed as (
        select
        order_sk
        , order_id
        , ship_country
        from staging
    )
    select * from transformed