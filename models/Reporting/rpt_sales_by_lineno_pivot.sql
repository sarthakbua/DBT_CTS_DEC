{{config(materialized = 'view' ,schema= 'reporting')}}

{% set linenos =  get_linenos() %}
 
select
orderid,
 
{% for lno in linenos %}
 
sum(case when lineno = {{lno}} then linesaleamount end) as lineno{{lno}}_sales,
 
{% endfor %}
 
sum(linesaleamount) as total_sales
from {{ ref('fct_orders') }}
group by 1
