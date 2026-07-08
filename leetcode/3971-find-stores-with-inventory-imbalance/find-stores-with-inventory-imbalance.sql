# Write your MySQL query statement below

with temp as
(
    select t1.store_id , max(t2.store_name) as store_name, max(t2.location) as location,
        MAX(t1.price) as max_price, MIN(t1.price) as min_price, count(distinct inventory_id) as count_item
    from inventory t1
        left join stores t2 on t1.store_id = t2.store_id
    group by t1.store_id
)
select t1.store_id, t1.store_name, t1.location, t2.product_name as most_exp_product,
    t3.product_name as cheapest_product, round(t3.quantity/t2.quantity,2) as imbalance_ratio  
from temp t1
    left join inventory t2 on t1.store_id = t2.store_id and t1.max_price = t2.price
    left join inventory t3 on t1.store_id = t3.store_id and t1.min_price = t3.price
where t1.count_item >= 3 and round(t3.quantity/t2.quantity,2) > 1
order by round(t3.quantity/t2.quantity,2) desc,t1.store_name