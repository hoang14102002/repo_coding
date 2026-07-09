# Write your MySQL query statement below

with temp as
(
    select user_id, count(distinct content_id) as total_reaction
    from reactions
    group by user_id
    having count(distinct content_id) >= 5
),
result as
(
    select t1.user_id, t1.reaction as dominant_reaction, 
        round(count(distinct t1.content_id) / t2.total_reaction,2) as reaction_ratio 
    from reactions t1
        left join temp t2 on t1.user_id = t2.user_id
    group by t1.user_id, t1.reaction
)
select * from result
where reaction_ratio > 0.6
order by reaction_ratio desc, user_id
