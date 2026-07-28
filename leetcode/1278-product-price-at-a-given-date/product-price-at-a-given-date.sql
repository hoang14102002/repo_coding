# Write your MySQL query statement below

with dim_item as
(
    select distinct product_id from products
),
filter_ as
(
    select *, row_number() over(partition by product_id order by change_date desc) as rn
    from products
    where change_date <= '2019-08-16'
)
select t1.product_id, ifnull(t2.new_price,10) as price
from dim_item t1
    left join filter_ t2 on t1.product_id = t2.product_id and t2.rn = 1