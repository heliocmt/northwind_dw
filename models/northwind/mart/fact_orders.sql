{{ config(materialized='table') }}

with
    customers as (
        select 
        customer_sk,
        customer_id
        from {{ ref('dim_customers') }}
    ),

    products as (
        select *
        from {{ ref('dim_products') }}
    ),

    employees as (
        select *
        from {{ ref('dim_employees') }}
    ),    

    suppliers as (
        select *
        from {{ ref('dim_suppliers') }}
    ),
    
    country as (
        select *
        from {{ ref('dim_country') }}
    ),

    order_with_sk as (
        select
        orders.order_id
        , customers.customer_sk
        , orders.order_date  
        , orders.shipped_date 
        , orders.required_date  
        , orders.freight 
        , orders.ship_name  
        , orders.ship_address 
        , orders.ship_city 
        , orders.ship_region  
        , orders.ship_postal_code 
        , orders.ship_country
        from {{ ref('stg_orders') }} as orders 
        left join customers customers on orders.customer_id = customers.customer_id
        left join employees employees on orders.employee_id = employees.employee_id
        left join country country on orders.ship_country = country.ship_country
    )

    select * from order_with_sk