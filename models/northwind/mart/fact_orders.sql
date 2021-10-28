{{ config(materialized='table') }}

with
    customers as (
        select 
        customer_sk
        , customer_id
        from {{ ref('dim_customers') }}
    ),

    products as (
        select *
        from {{ ref('dim_products') }}
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
    
    order_date as (
        select
        order_date_sk
        , order_id
        from {{ ref('dim_date') }}
    ),

    order_with_sk as (
        select
        orders.order_id
        , customers.customer_sk as customer_fk
        , order_date.order_date_sk as order_date_fk
        , employees.employee_sk as employee_fk
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
        left join customers on orders.customer_id = customers.customer_id
        left join employees on orders.employee_id = employees.employee_id
        left join order_date on orders.order_id = order_date.order_id
    ),
    
    order_details_with_sk as (
        select
        products.product_sk as product_fk
        , suppliers.supplier_sk as supplier_fk
        , products.product_id as product_id
        , suppliers.supplier_id
        , order_details.order_id as order_id
        , order_details.unit_price as unit_price
        , order_details.quantity as quantity
        , order_details.discount as discount
        from {{ ref('stg_order_details') }} as order_details_with_sk
        left join products on order_details_with_sk.product_id = products.product_id
        left join suppliers on order_details_with_sk.supplier_id = suppliers.supplier_id 
    ),
    
    final as (
        select
        order_details_with_sk.order_id
        , order_with_sk.customer_fk 
        , order_details_with_sk.product_fk
        , order_with_sk.order_fk
        , order_with_sk.order_date_fk
        , order_with_sk.employee_fk
        , order_with_sk.supplier_fk
        , order_with_sk.order_date  
        , order_with_sk.shipped_date 
        , order_with_sk.required_date  
        , order_with_sk.freight 
        , order_with_sk.ship_name  
        , order_with_sk.ship_address 
        , order_with_sk.ship_city 
        , order_with_sk.ship_region  
        , order_with_sk.ship_postal_code 
        , order_with_sk.ship_country
        , order_details_with_sk.unit_price 
        , order_details_with_sk.quantity 
        , order_details_with_sk.discount  
        from order_with_sk as final
        left join order_details_with_sk on final.order_id = order_details_with_sk.order_id       
    )
 select * from final