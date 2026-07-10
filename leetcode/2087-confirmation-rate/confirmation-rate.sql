# Write your MySQL query statement below

    select t1.user_id,
       ifnull(round(sum(case when t2.action = 'confirmed' then 1 else 0 end)/count(t2.action),2),0) as confirmation_rate 
    from signups t1
        left join confirmations t2 on t1.user_id = t2.user_id 
    group by t1.user_id