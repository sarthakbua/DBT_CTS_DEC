{{config(materialized ='table', schema = 'transforming') }}

select
c.customerid,
c.CompanyName,
c.ContactName,
c.city,
c.country,
d.divisionname,
c.address,
c.fax,
c.phone,
c.PostalCode,
IFF (c.StateProvince = '','NA', c.StateProvince) as statename

from

{{ref('stg_customers')}} as c

inner join

{{ref('lkp_divisions')}} as d

on c.DivisionID =d.DivisionID