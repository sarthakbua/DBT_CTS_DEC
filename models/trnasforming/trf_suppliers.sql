{{ config(materialized = 'table', schema = 'transforming') }}
 
select 
GET ( XMLGET(supplier_info , 'SupplierID'), '$') as SupplierID,
GET ( XMLGET(supplier_info , 'CompanyName'), '$')::varchar as CompanyName,
GET ( XMLGET(supplier_info , 'ContactName'), '$')::varchar as ContactName,
GET ( XMLGET(supplier_info , 'Address'), '$')::varchar as Address,
GET ( XMLGET(supplier_info , 'City'), '$')::varchar as City,
GET ( XMLGET(supplier_info , 'Phone'), '$')::varchar as Phone,
GET ( XMLGET(supplier_info , 'Fax'), '$')::varchar as Fax,
GET ( XMLGET(supplier_info , 'Country'), '$')::varchar as country

from {{ref('stg_suppliers')}}  