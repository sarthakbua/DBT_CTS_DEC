import snowflake.snowpark.functions as F
import pandas as pd 
import holidays

def avg_sale( x,y):
    return y/x

def is_holiday(date_col):
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday


def model(dbt, session):

    dbt.config(materilized ='table', schema ='reporting', packages = ['holidays'] ) 
    dim_customers_df = dbt.ref('dim_customers')

    fact_orders_df = dbt.ref('fct_orders')
 
    customers_orders_df =( 
                           fact_orders_df
                          .group_by('customerid')
                          .agg
                            (
                            F.min(F.col('orderdate')).alias('first_orderdate'),
                            F.max(F.col('orderdate')).alias('recent_orderdate'),
                            F.count(F.col('orderid')).alias('total_orders'),
                            F.sum(F.col('linesaleamount')).alias('total_sales'),
                            F.avg(F.col('margin')).alias('avg_margin')
                            )
                        )

    final_df  = (
                dim_customers_df
                .join(customers_orders_df,customers_orders_df.customerid == dim_customers_df.customerid,'left')
                .select(dim_customers_df.companyname.alias('companyname'),
                        dim_customers_df.contactname.alias('contactname'),
                        customers_orders_df.first_orderdate.alias('first_orderdate'),
                        customers_orders_df.recent_orderdate.alias('recent_orderdate'),
                        customers_orders_df.total_orders.alias('total_orders'),
                        customers_orders_df.total_sales.alias('total_sales'),
                        customers_orders_df.avg_margin.alias('avg_margin')

                )   
                ) 

    final_df = final_df.withColumn("avg_salevalue", avg_sale(final_df["total_orders"], final_df["total_sales"]))      
  
    final_df = final_df.filter(F.col("FIRST_ORDERDATE").isNotNull())
    final_df = final_df.to_pandas()
    
    final_df["IS_FIRST_ORDERDATE_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)
    return final_df

     