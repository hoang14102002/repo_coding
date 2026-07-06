# Write your MySQL query statement below

with temp as
(
    select t1.*, t2.category 
    from productpurchases t1
        left join productinfo t2 on t1.product_id = t2.product_id
)
select a.category as category1, b.category as category2 ,
    count(distinct a.user_id) as customer_count
from temp a
    left join temp b on a.user_id = b.user_id  
where a.category < b.category
group by a.category, b.category
having count(distinct a.user_id) >= 3
order by count(distinct a.user_id) desc, a.category, b.category