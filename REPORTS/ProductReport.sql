/*
PRODUCT REPORT
--This report has product metrics and behaviors
 1. Include important attributes: name, category, subcategory and cost.
 2. Divide products into categories such as High-Perfomers, Mid-Range, Or Low-Perfomers
 3. Aggregate product-level metrics:
    -total orders
    -total sales
    -total quantity sold
    -total customers(unique)
    -lifespan(months)
 4. Calculate KPIs:
    -Average order revenue
    -Average monthly revenue
    -Months since last sale
*/
create OR ALTER view gold.report_products as
with base_query as
(
select
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost
from gold.fact_sales f
left join gold.dim_products p
on p.product_key=f.product_key
where f.order_date is not null
),
product_aggregation as(
select
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct customer_key) as total_customers,
MAX(order_date) as last_Sale_date,
DATEDIFF(month, min(order_date), max(order_date)) as lifespan,
ROUND(AVG(CAST(sales_amount AS FLOAT)/ NULLIF(quantity,0)),1) as avg_selling_price,
product_key,
product_name,
category,
subcategory,
cost
from base_query
group by
product_key,
product_name,
category,
subcategory,
cost
)
Select
product_key,
product_name,
category,
subcategory,
cost,
last_sale_date,
datediff(month, last_sale_date, getdate()) as recency,
case 
when total_sales>50000 then 'High-Perfomer'
when total_sales>=10000 then 'Mid-Range'
else 'Low-Perfomer'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
case
when total_orders=0 then 0
else total_sales/total_orders
end as average_order_revenue,
case 
when lifespan=0 then total_sales
else total_sales/lifespan
end as average_monthly_revenue
from product_aggregation;