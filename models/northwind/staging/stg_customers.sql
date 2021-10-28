with source_data as (
        select
        customer_id
        , city
        , postal_code
        , region
        , address
        , contact_name
        , phone
        , company_name
        , contact_title
        from {{ source('erp_northwind','customers')}}
    )

select * from source_data