with source_data as (
        select
        ship_country
        , order_id
        , customer_id
    
        from {{ source('erp_northwind','country')}}
    )

select * from source_data