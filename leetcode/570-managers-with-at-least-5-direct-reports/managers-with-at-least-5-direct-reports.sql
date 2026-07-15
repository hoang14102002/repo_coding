# Write your MySQL query statement below

    select t1.name
    from employee t1
        left join employee t2 on t1.id = t2.managerId
    group by t1.id
    having count(t2.managerId) >= 5