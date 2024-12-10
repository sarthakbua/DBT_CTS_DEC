{{config(materialized = 'view' ,schema= 'reporting')}}

  
select c.companyname,c.contactName, min(o.orderdate) as first_order_date,
max(o.orderdate) as recent_order_date, count(o.orderid) as ordercount, 
sum(o.linesaleamount) as sales , avg(o.margin) as avg_margin
 from 
 {{ref('dim_customers')}} as c inner join
 {{ref('fct_orders')}} as o
 on  c.customerid =o.customerid  
 group by c.companyname,c.contactName
 order by sales desc 