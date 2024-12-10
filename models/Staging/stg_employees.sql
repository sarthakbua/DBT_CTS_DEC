{{config (materialized ='table',schema = env_var('DBT_STGSCHEMA','STAGING_DEV'))}}

select 
*
from  
{{ source('qwt_raw','employees')}}