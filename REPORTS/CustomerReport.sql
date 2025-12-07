/*
CUSTOMER REPORT
 --This report has customer metrics and behaviors
 1. Include important attributes: names, ages and transaction details.
 2. Divide customers into categories such as VIP, Regular, New 
 3. Aggregate customer-level metrics:
    -total orders
    -total sales
    -total quantity purchased
    -total products
    -lifespan(months)
 4. Calculate KPIs:
    - Average order value
    -Average monthly spend
    -Months since last order
*/
create or alter view gold.report_customers as
with base_query as(
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
c.country,
concat(c.first_name,'',c.last_name) as customer_name,
datediff(year,c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
where f.order_date is not null
),
customer_aggregation as(
select 
customer_key,
customer_number,
customer_name,
country,
customer_age,
count(distinct order_number) as total_orders,
SUM(sales_amount) as total_sales,
SUM(quantity) as total_quantity,
COUNT(DISTINCT product_key) as total_products,
MAX(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as lifespan
from base_query
GROUP BY 
customer_key,
customer_number,
customer_name,
country,
customer_age
)
select 
customer_key,
customer_number,
customer_name,
country,
customer_age,
case 
 when customer_age<20 then 'Under 20'
 when customer_age between 20 and 29 THEN '20-29'
 when customer_age between 30 and 39 THEN '30-39'
 when customer_age between 40 and 49 THEN '40-49'
 else '50 and above'
end as age_group,
case
 when lifespan>=12 AND total_sales>=5000 THEN 'VIP'
 when lifespan>=12 AND total_sales<=5000 THEN 'Regular'
 ELSE 'NEW'
END AS customer_segment,
datediff(month, last_order_date, getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
lifespan,
case 
 when total_orders=0 THEN 0
 else total_sales/total_orders 
end as average_order_value,
case 
  when lifespan =0 THEN total_sales
  else total_sales/lifespan
end as average_monthly_spend
FROM customer_aggregation
