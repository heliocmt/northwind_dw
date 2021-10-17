with source_data as (
        select
        city
        , fax
        , postal_code
        , region
        , address
        , customer_id
        , contact_name
        , phone
        , company_name
        , contact_title
    
        from {{ source('erp_northwind','customers')}}
    )

select * from source_data