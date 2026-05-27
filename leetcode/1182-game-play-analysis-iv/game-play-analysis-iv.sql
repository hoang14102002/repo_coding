# Write your MySQL query statement below

    with temp as
    (
        select *, row_number() over(partition by player_id order by event_date) as sort_date,
            datediff(lead(event_date) over(partition by player_id order by event_date), event_date) as diff
        from activity
    )
    select round(sum(case when diff = 1 and sort_date = 1 then 1 else 0 end)/ count(distinct player_id),2) as fraction  
    from temp