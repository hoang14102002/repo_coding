# Write your MySQL query statement below

with temp as
(
    select c.name, count(distinct movie_id) as total_rating
    from movierating a
        left join users c on a.user_id = c.user_id
    group by c.user_id
),
temp2 as
(
    select c.title, avg(a.rating) as avg_rating
    from movierating a 
        left join movies c on a.movie_id = c.movie_id
    where month(a.created_at) = 2 and year(a.created_at) = 2020 
    group by a.movie_id
),
filter_temp as
(
    select name from temp
    order by total_rating desc, name
    limit 1
),
filter_temp2 as
(
    select title from temp2
    order by avg_rating desc, title
    limit 1
)
select name as results from filter_temp 
union all
select * from filter_temp2