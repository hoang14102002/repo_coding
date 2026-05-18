# Write your MySQL query statement below
    with temp as 
    (
        select *, dense_rank() over(order by salary desc) as rn
        from employee
    )
    select case when max(rn) < 2 then null else salary end as SecondHighestSalary  
    from temp where rn = 2;
   