# Write your MySQL query statement below
    with temp as 
    (
        select session_id, user_id, 
        TIMESTAMPDIFF(MINUTE,  min(event_timestamp), max(event_timestamp)) as session_duration_minutes,
        sum(case when event_type = 'scroll' then 1 else 0 end) as scroll_count,
        sum(case when event_type = 'click' then 1 else 0 end) as click_count,
        sum(case when event_type = 'purchase' then 1 else 0 end) as have_purchase
    from app_events 
    group by session_id, user_id
    )
    select session_id, user_id, session_duration_minutes, scroll_count 
    from temp
    where session_duration_minutes > 30 and scroll_count >= 5 and have_purchase = 0 and click_count/scroll_count<0.2
    order by scroll_count desc, session_id
    