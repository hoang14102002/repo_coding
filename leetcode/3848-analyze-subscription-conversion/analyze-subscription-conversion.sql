# Write your MySQL query statement below

    select user_id,
        round(sum(case when activity_type  = 'free_trial' then activity_duration else 0 end)/
            sum(case when activity_type  = 'free_trial' then 1 else 0 end),2) as trial_avg_duration,
        round(sum(case when activity_type  = 'paid' then activity_duration else 0 end)/
            sum(case when activity_type  = 'paid' then 1 else 0 end)
            ,2) as paid_avg_duration 
    from useractivity
    group by user_id
    having  round(sum(case when activity_type  = 'paid' then activity_duration else 0 end)/
            sum(case when activity_type  = 'paid' then 1 else 0 end)
            ,2) > 0
    order by user_id