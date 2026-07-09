# Write your MySQL query statement below

with temp as
(
    select *, row_number() over(partition by employee_id order by review_date desc) as rn
    from performance_reviews 
),
check_ as
(
    select *, rating - lead(rating) over(partition by employee_id order by review_date desc) as prv
    from temp
    where rn <= 3
),
filter_employee as
(
    select distinct employee_id from check_
    where prv = 0
),
result as
(
    select t1.employee_id, max(t2.name) as name, 
        sum(case when t1.rn = 1 then t1.rating else 0 end) -
        sum(case when t1.rn = 3 then t1.rating else 0 end) as improvement_score
    from check_ t1
        left join employees t2 on t1.employee_id = t2.employee_id
    group by t1.employee_id
    having sum(case when t1.rn = 3 then t1.rating else 0 end) <> 0
)
select * from result 
where improvement_score > 0
and employee_id not in (select employee_id from filter_employee)
order by improvement_score desc, name

