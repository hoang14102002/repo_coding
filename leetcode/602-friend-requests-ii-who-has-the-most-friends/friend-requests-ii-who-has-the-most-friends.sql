# Write your MySQL query statement below

with temp as
(
    select requester_id as user_id from RequestAccepted 
    union all
    select accepter_id from RequestAccepted 
)
select user_id as id, count(user_id) as num 
from temp
group by user_id
order by count(user_id) desc
limit 1
