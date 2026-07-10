# Write your MySQL query statement below

with temp as
(
    select count(distinct product_key) as total_product from product
),
cal_ as
(
    select customer_id, count(distinct product_key) as cal_product from customer 
    group by customer_id
)
select distinct t1.customer_id from cal_ t1
    inner join temp t2 on t1.cal_product = t2.total_product