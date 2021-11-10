{{ config(materialized='table') }}

with
    customers as (
        select 
        customer_sk
        , customer_id
        from {{ ref('dim_customers') }}
    ),

    employees as (
        select 
        employee_sk
        , employee_id
        , first_name
        , last_name
        from {{ ref('dim_employees') }}
    ),    

    suppliers as (
        select 
        supplier_sk
        , supplier_id
        , company_name
        from {{ ref('dim_suppliers') }}
    ),

    order_date1 as (
        select
        order_date_sk
        , order_date
        from {{ ref('dim_date') }}
    ),

    order_with_sk as (
        select distinct
        order_id 
        , customers.customer_sk as customer_fk
        , employees.employee_sk as employee_fk
        , order_date1.order_date
        , shipped_date 
        , required_date
        , freight
        , ship_name
        , ship_address
        , ship_city
        , ship_region
        , ship_postal_code
        , ship_country
        from {{ ref('stg_orders') }} as order_with_sk
        left join customers on order_with_sk.customer_id = customers.customer_id
        left join employees on order_with_sk.employee_id = employees.employee_id
        left join order_date1 on order_with_sk.order_date = order_date1.order_date
        order by order_id asc)

 select * from order_with_sk