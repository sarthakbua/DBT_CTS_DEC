{{config (materialized ='table', schema = env_var('DBT_STGSCHEMA','STAGING_DEV'))}}

select 
ProductID  ,
  ProductName  ,
  SupplierID  ,
  CategoryID  ,
  QuantityPerUnit  ,
  UnitCost   ,
  UnitPrice   ,
  UnitsInStock  ,
  UnitsOnOrder 
from  
{{ source('qwt_raw','products')}}