# Write your MySQL query statement below

    with temp as
    (
        select *, sum(weight) over(order by turn) total_weight
        from queue
    )
    select person_name from temp
    where total_weight <= 1000
    order by turn desc
    limit 1
   