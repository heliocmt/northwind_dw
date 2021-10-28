{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_employees') }}
    ),
    transformed as (
        select
        row_number() over (order by employee_id) as employee_sk
        , employee_id
        , first_name
        , last_name
        , title
        , birth_date
        , hire_date
        , address
        , city
        , country
        , home_phone
        from staging
    )
select * from transformed