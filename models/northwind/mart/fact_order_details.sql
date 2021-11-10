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

    suppliers as (
        select 
        supplier_sk
        , supplier_id
        , company_name
        from {{ ref('dim_suppliers') }}
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
        , products.units_in_stock as units_in_stock
        , products.units_on_order as units_on_order
        from {{ ref('stg_order_details') }} as order_details
        left join products on order_details.product_id = products.product_id
    ),

    order_details_with_sk as (
        select 
        order_details.order_id as order_id
        , order_details.product_sk as product_fk
        , order_details.product_name
        , order_details.unit_price 
        , order_details.quantity  
        , order_details.discount 
        , order_details.category_id
        , order_details.quantity_per_unit
        , order_details.units_in_stock
        , order_details.units_on_order
        , supplier_sk as supplier_fk
        , company_name
        from suppliers as order_details_with_sk
        left join order_details on order_details_with_sk.supplier_id = order_details.supplier_id 
        order by order_id asc
    )
    select * from order_details_with_sk