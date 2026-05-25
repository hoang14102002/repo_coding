# Write your MySQL query statement below
# Write your MySQL query statement below
    with temp as 
    (
        select *, id - row_number() over (order by id) as grp
        from stadium 
        where people >= 100
    ),
    seq as (
        select
            *,
            count(*) over (partition by grp) AS seq_len
        from temp
    )
    select id,visit_date,people
    from seq where seq_len >= 3
    order by visit_date
    
    