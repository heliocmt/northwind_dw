    with
        source_data as (
            select
            order_id
            , product_id 
            , unit_price 
            , quantity 
            , discount 
            from {{ source('erp_northwind','order_details')}}
        )
        select * from source_data
