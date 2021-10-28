with 
source_data as (
       select
       supplier_id
       , company_name
       , contact_name
       , contact_title
       , address
       , city
       , country
       , phone
       from {{ source('erp_northwind','suppliers')}}
    )

select * from source_data