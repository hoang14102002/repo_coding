# Write your MySQL query statement below

with temp as
(
    select product_id, min(year) as min_year from sales
    group by product_id
)
select t1.product_id, t1.year as first_year, t1.quantity, t1.price 
from sales t1
    inner join temp t2 on t1.product_id = t2.product_id and t1.year = t2.min_year