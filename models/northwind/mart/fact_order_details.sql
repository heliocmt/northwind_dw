{{ config(materialized='table') }}

with
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
        from {{ ref('dim_products') }}
    ),

    order_details as (
        select 
        order_id
        , products.product_sk 
        , products.product_name
        , products.unit_price 
        , quantity  
        , discount  
        , products.supplier_id 
        , products.category_id 
        , products.quantity_per_unit
        , products.units_in_stock 
        , products.units_on_order 
        from {{ ref('stg_order_details') }} as order_details
        left join products on order_details.product_id = products.product_id
        order by order_details.order_id
    )
    select * from order_details