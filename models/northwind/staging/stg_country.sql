with source_data as (
        select
        row_number() over (order by order_id) as order_sk
        , order_id
        , ship_country
        , customer_id
        from {{ source('erp_northwind','country')}}
    )

select * from source_data