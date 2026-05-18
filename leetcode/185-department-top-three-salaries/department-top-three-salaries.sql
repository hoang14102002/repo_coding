# Write your MySQL query statement below
    with temp as
    (
        select b.name as Department
            , a.name as Employee
            , a.salary as Salary
            , dense_rank() over(partition by b.id order by a.salary desc) as rn
        from employee a 
            left join department b on a.departmentId = b.id
    )
    select Department, Employee, Salary from temp where rn <= 3