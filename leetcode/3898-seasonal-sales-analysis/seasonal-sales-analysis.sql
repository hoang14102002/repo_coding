# Write your MySQL query statement below

with temp as
(
    select
        case when month(t1.sale_date) in (3,4,5) then 'Spring'
            when month(t1.sale_date) in (6,7,8) then 'Summer'
            when month(t1.sale_date) in (9,10,11) then 'Fall'
            else 'Winter' end as season ,
        t2.category,
        sum(t1.quantity) as total_quantity,
        sum(t1.quantity*t1.price) as total_revenue,
        row_number() over(partition by case when month(t1.sale_date) in (3,4,5) then 'Spring'
            when month(t1.sale_date) in (6,7,8) then 'Summer'
            when month(t1.sale_date) in (9,10,11) then 'Fall'
            else 'Winter' end order by sum(t1.quantity) desc, sum(t1.quantity*t1.price) desc, t2.category desc) as rn
    from sales t1
        left join products t2 on t1.product_id = t2.product_id
    group by case when month(t1.sale_date) in (3,4,5) then 'Spring'
            when month(t1.sale_date) in (6,7,8) then 'Summer'
            when month(t1.sale_date) in (9,10,11) then 'Fall'
            else 'Winter' end,t2.category
)
select season, category, total_quantity, total_revenue-- , rn
from temp
where rn = 1
order by season
