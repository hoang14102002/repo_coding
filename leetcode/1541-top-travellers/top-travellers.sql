# Write your MySQL query statement below

    select max(t1.name) as name, ifnull(sum(t2.distance),0) as travelled_distance
    from users t1
        left join rides t2 on t2.user_id = t1.id
    group by t1.id
    order by ifnull(sum(t2.distance),0) desc,max(t1.name)