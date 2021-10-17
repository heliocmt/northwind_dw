{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_customers') }}
    ),
    transformed as (
        select
        row_number() over (order by customer_id) as customer_sk
        , city
        , fax
        , postal_code
        , region
        , address
        , customer_id
        , contact_name
        , phone
        , company_name
        , contact_title
    
        from staging
    )
    select * from transformed