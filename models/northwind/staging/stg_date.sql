with source_data as (
        select
        row_number() over (order by order_date) as order_date_sk
        , order_date
        , order_id
        from {{ source('erp_northwind','date')}}
    )

select * from source_data