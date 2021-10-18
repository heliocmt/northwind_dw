with 
source_data as (
       select
       employee_id 
       , first_name
       , last_name
       , title
       , title_of_courtesy
       , birth_date
       , hire_date
       , address
       , city
       , country
       , home_phone
       from {{ source('erp_northwind','employees')}}
    )

select * from source_data