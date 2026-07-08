# Write your MySQL query statement below

with temp as
(
    select *,
        row_number() over(partition by student_id, subject order by exam_date) as first_time,
        row_number() over(partition by student_id, subject order by exam_date desc) as last_time
    from scores
)
    select t1.student_id, t1.subject, t1.score as first_score,
        t2.score as latest_score
    from temp t1
        left join temp t2 on t1.student_id = t2.student_id and t1.subject = t2.subject and t2.last_time = 1
    where t1.first_time = 1
        and t1.score < t2.score
    order by t1.student_id, t1.subject