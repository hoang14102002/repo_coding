# Write your MySQL query statement below

    with valid_user as
    (
        select user_id, count(distinct course_id) as course_count,
            avg(course_rating) as avg_course_rating
        from course_completions
        group by user_id
        having count(distinct course_id) >= 5 and avg(course_rating) >= 4
    ),
    fillter as
    (
        select a.user_id, a.course_name, row_number() over(partition by user_id order by completion_date) as rn
        from course_completions a
            inner join valid_user b on a.user_id = b.user_id
    )
    select t1.course_name as first_course, t2.course_name as second_course,
        count(distinct t1.user_id) as transition_count
    from fillter t1
        inner join fillter t2 on t1.user_id = t2.user_id and t1.rn +1 = t2.rn
    group by t1.course_name, t2.course_name
    order by count(distinct t1.user_id)  desc, t1.course_name, t2.course_name