{{ config(materialized='table') }}

with
    customers as (
        select 
        customer_sk
        , customer_id
        from {{ ref('dim_customers') }}
    ),

    products as (
        select
        product_sk 
        , product_id 
        , product_name 
        , supplier_id 
        , category_id 
        , quantity_per_unit 
        , unit_price 
        , units_in_stock 
        , units_on_order 
        , reorder_level 
        , discontinued
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
        , order_date
        , order_id
        from {{ ref('dim_date') }}
    ),

    order_with_sk as (
        select
        customers.customer_sk as customer_fk
        , order_date.order_date_sk as order_date_fk
        , employees.employee_sk as employee_fk
        , order_date.order_id as order_id
        , order_date  
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
        left join order_date on order_with_sk.order_date = order_date.order_date
    ),
    
    order_details as (
        select 
        order_id
        , products.product_sk as product_sk
        , products.product_name as product_name
        , products.unit_price as unit_price
        , quantity  
        , discount  
        , products.supplier_id as supplier_id
        , products.category_id as category_id
        , products.quantity_per_unit as quantity_per_unit
        , products.units_in_stock as unit_in_stock
        , products.units_on_order as unit_on_order
        from {{ ref('stg_order_details') }} as order_details
        left join products on order_details.product_id = products.product_id
    ),

    order_details_with_sk as (
        select 
        order_details.order_id
        , order_details.product_sk as product_fk
        , order_details.product_name
        , order_details.unit_price 
        , order_details.quantity  
        , order_details.discount 
        , order_details.category_id
        , order_details.quantity_per_unit
        , order_details.unit_in_stock
        , order_details.unit_on_order
        , supplier_sk as supplier_fk
        , company_name
        from suppliers as order_details_with_sk
        left join order_details on order_details_with_sk.supplier_id = order_details.supplier_id 
    ),
    
    final as (
        select
        order_details_with_sk.order_id as order_id
        , customer_fk 
        , order_details_with_sk.product_fk
        , order_date_fk
        , employee_fk
        , supplier_fk
        , shipped_date 
        , required_date  
        , freight 
        , ship_name  
        , ship_address 
        , ship_city 
        , ship_region  
        , ship_postal_code 
        , ship_country
        , order_details_with_sk.unit_price 
        , order_details_with_sk.quantity 
        , order_details_with_sk.discount
        , order_details_with_sk.unit_in_stock
        , order_details_with_sk.unit_on_order  
        from order_with_sk as final
        left join order_details_with_sk on final.order_id = order_details_with_sk.order_id       
    )
 select * from final