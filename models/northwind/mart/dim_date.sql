{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_date') }}
    ),
    transformed as (
        select
        order_date_sk
        , order_date
        , order_id
        from staging
    )
    select * from transformed