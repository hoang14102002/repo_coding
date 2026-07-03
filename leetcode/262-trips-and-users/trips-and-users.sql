# Write your MySQL query statement below
with filter_ban as 
(
    select a.* from trips a 
        left join users b on a.client_id = b.users_id
        left join users c on a.driver_id = c.users_id
    where b.banned = "No" and c.banned = "No"
)
select request_at as "Day",
    round(sum(case when status like "cancel%" then 1 else 0 end)/count(id),2) as  "Cancellation Rate"
from filter_ban
where request_at between "2013-10-01" and "2013-10-03"
group by request_at
order by request_at