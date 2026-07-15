# Write your MySQL query statement below

with temp as
(
    select *,
        row_number() over(partition by customer_id order by order_date) as rn
    from delivery
)
select round(sum(case when order_date = customer_pref_delivery_date then 1 else 0 end)/
    count(distinct customer_id)*100,2) as immediate_percentage 
from temp 
where rn = 1