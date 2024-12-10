{{config(materialized ='table', schema = 'transforming') }}

select
emp.empid,
emp.lastname,
emp.firstname,
emp.title,
emp.hiredate,
emp.extension,
iff (mgr.firstname is null,emp.firstname, mgr.firstname ) as manager,
emp.yearsalary,
off.officecity,
off.OfficeCountry


 from
{{ref('stg_employees')}} as emp left join
{{ref('stg_employees')}} as mgr
on emp.reportsto =mgr.empid
inner join {{ref('lkp_offices')}} as off
on emp.office = off.office