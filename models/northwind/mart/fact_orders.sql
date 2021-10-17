{{ config(materialized='table') }}

with
    customers as (
        select 
        customer_sk,
        customer_id
        from {{ ref('dim_customers') }}
    ),

    order_with_sk as (
        select
        orders.order_id as pedidos ,
        customers.customer_id as clientes ,
        orders.order_date as data_pedido ,
        orders.shipped_date as data_envio ,      
        orders.required_date as prazo_entrega ,
        orders.freight as frete ,
        orders.ship_name as transportadora ,
        orders.ship_address as endereco ,
        orders.ship_city as cidade ,
        orders.ship_region as regiao ,
        orders.ship_postal_code as cep ,
        orders.ship_country as pais

    from {{ ref('stg_orders') }} orders 
    left join customers customers on orders.customer_id = customers.customer_id
    )
    select * from order_with_sk