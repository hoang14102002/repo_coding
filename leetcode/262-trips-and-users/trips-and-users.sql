# Write your MySQL query statement below
with 
banned_client_users as
(
    select * from users where role = 'client' and banned = 'yes'
),
banned_driver_users as
(
    select * from users where role = 'driver' and banned = 'yes'
)
,filter_band as 
(
    select a.* 
    from trips a 
        left join banned_client_users b on a.client_id = b.users_id
        left join banned_driver_users c on a.driver_id = c.users_id
    where b.users_id is null and c.users_id is null
)
select request_at as 'Day', 
    round(sum(case when status like 'cancelled%' then 1 else 0 end)/ count(distinct id),2) as 'Cancellation Rate'
from filter_band
where request_at between '2013-10-01' and '2013-10-03'
group by request_at