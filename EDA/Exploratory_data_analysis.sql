/* Exploring the data in the Tables */

USE DATAWAREHOUSE;

SELECT * FROM INFORMATION_SCHEMA.TABLES;

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';

-- Explore All countries from our customers table
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all categories 'The Major Divisions'
select distinct category,subcategory, product_name from gold.dim_products
order by 1,2,3;

--Explore the dates--
--How many years or months of sales are avaiable--
select 
min(order_date) as first_order_date, 
max(order_date) as last_order_date,
datediff(year, min(order_date),max(order_date)) as order_range_years
from gold.fact_sales;

select 
min(order_date) as first_order_date, 
max(order_date) as last_order_date,
datediff(MONTH, min(order_date),max(order_date)) as order_range_months
from gold.fact_sales;

--Find the oldest and youngest customer--
select
Min(birthdate) as  oldest,
datediff(year, min(birthdate), getdate()) as oldest,
max(birthdate) as younges,
datediff(year, max(birthdate), getdate()) as youngest
from gold.dim_customers


/*	Find How many items are sold
	Find the averge selling price
	FInd the total number of orders
	find the total number of products
	find the total number of customer
	find the total number of customers that has places an order
*/

select sum(sales_amount) as total_sales from gold.fact_sales;
select sum(quantity) as total_quatity from gold.fact_sales;
select AVG(price) as avg_price from gold.fact_sales;
select COUNT(order_number) as total_orders from gold.fact_sales;
select COUNT(distinct order_number) as total_orders from gold.fact_sales;
select * from gold.fact_sales;
select count(product_key) as total_product from gold.dim_products;
select count(distinct product_key) as total_product from gold.dim_products;
select count(customer_key) as total_customers from gold.dim_customers;
select count(distinct customer_key) as total_customers from gold.fact_sales;

--Generate a report that shows all key metrics of the business--

select ('Total Sales') as measure_name, sum(sales_amount) as measure_value from gold.fact_sales
union all
select 'Total_Quantity' as measure_name, sum(quantity) as measure_value from gold.fact_sales
union all
select 'Average Price', avg(price) from gold.fact_sales
union all
select 'Total Num. Orders', count(distinct order_number) from gold.fact_sales
union all
select 'Total Num. Products', count(product_name)  from gold.dim_products
union all
select 'Total Num. Customers', count(customer_key) from gold.fact_sales;


/*
Find total customers by countries
Find total customers by gender
find total products by category
what is the average cost in each category
what is the total revenue generated for each customer
find total revenue is generated by each customer
what is the distribution of solld items across countries
*/

select
country, 
count(customer_key) as total_customers
from gold.dim_customers
group by country
order by total_customers desc

select
gender, 
count(customer_key) as total_customers
from gold.dim_customers
group by gender
order by total_customers desc

select 
category,
count(product_key) as total_products
from gold.dim_products
group by category
order by total_products desc

select 
category,
avg(cost) as avg_costs
from gold.dim_products
group by category
order by avg_costs desc


select 
p.category,
sum(f.sales_amount) total_revenue
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key = f.product_key
 group by p.category
 order by total_revenue desc

 select
 c.customer_key,
 c.first_name,
 c.last_name,
 sum(f.sales_amount) as total_revenue
 from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key= f.customer_key
 group by 
 c.customer_key,
 c.first_name,
 c.last_name
 order by total_revenue desc

 select
 c.country,
 sum(f.quantity) as total_sold_items
 from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key= f.customer_key
 group by 
 c.country
 order by total_sold_items desc

 /* Ranking Analysis
 Which 5 products generate the highest revenue

 What are the 5 worst-performing products in terms of sales
 
 */


select top 5
p.product_name,
sum(f.sales_amount) total_revenue
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key = f.product_key
 group by p.product_name
 order by total_revenue desc

 select top 5
p.subcategory,
sum(f.sales_amount) total_revenue
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key = f.product_key
 group by p.subcategory
 order by total_revenue desc


 --Window Function--
select *
from( 
	select
	p.product_name,
	sum(f.sales_amount) total_revenue,
	row_number() over (order by sum(f.sales_amount) desc) as rank_products
	 from gold.fact_sales f
	 left join gold.dim_products p
	 on p.product_key = f.product_key
	 group by p.product_name)t
 where rank_products <= 5



select top 5
p.product_name,
sum(f.sales_amount) total_revenue
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key = f.product_key
 group by p.product_name
 order by total_revenue

  select top 5
p.subcategory,
sum(f.sales_amount) total_revenue
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key = f.product_key
 group by p.subcategory
 order by total_revenue


 -- Find the Top 10 customers who have generate the highest revenue--
 select top 10
 c.customer_key,
 c.first_name,
 c.last_name,
 sum(f.sales_amount) as total_revenue
 from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key= f.customer_key
 group by 
 c.customer_key,
 c.first_name,
 c.last_name
 order by total_revenue desc



 -- The customers with the fewest orders placed--

select top 3
 c.customer_key,
 c.first_name,
 c.last_name,
 count(distinct order_number) as total_orders
 from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key= f.customer_key
 group by 
 c.customer_key,
 c.first_name,
 c.last_name
 order by total_orders
