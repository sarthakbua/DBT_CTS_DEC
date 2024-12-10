{{config (materialized ='table', schema = env_var('DBT_STGSCHEMA','STAGING_DEV'))}}

select 
CustomerID   ,
CompanyName  ,
ContactName   ,
City ,
Country  ,
DivisionID  ,
Address   ,
Fax  ,
Phone  ,
PostalCode    ,
StateProvince
from  
{{ source('qwt_raw','customers')}}