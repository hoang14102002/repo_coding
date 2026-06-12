with temp as 
(
    select t1.student_id, t1.subject, t1.session_date, t1.hours_studied,
    Row_number() over(partition by t1.student_id, t1.subject order by t1.session_date) as rn_first_cycle
    from study_sessions t1
),
order_in_a_cycle as 
(
    select *, 
     Row_number() over(partition by student_id, rn_first_cycle order by session_date) as       order_in_a_cycle
    from temp
), 
gap_day as
(
    select student_id, 
        IFNULL(datediff(session_date, 
                 lag(session_date) over (partition by student_id order by session_date)
        ),0) AS diff_day
    from study_sessions 
), max_gap_day as
(

    select student_id, max(diff_day) as diff_day
    from gap_day
    group by student_id
    having max(diff_day) <= 2
), valid_student as
(
select distinct a.student_id
from order_in_a_cycle a
    inner join order_in_a_cycle b on a.subject = b.subject AND a.student_id = b.student_id 
        AND a.order_in_a_cycle = b.order_in_a_cycle -- AND b.rn_first_cycle  <> 1
    inner join max_gap_day c on a.student_id = c.student_id
where A.rn_first_cycle = 1
group by a.student_id
having count(distinct a.subject) >= 3 AND MOD(COUNT(a.subject), MAX(b.order_in_a_cycle)) = 0    
    AND COUNT(a.subject)/MAX(b.order_in_a_cycle) >= 2
    -- nhiều hơn 3 môn và đủ vòng
)
select t1.student_id, max(t3.student_name) as student_name,
    max(t3.major) as major, count(distinct t1.subject) as cycle_length, 
    sum(t1.hours_studied) as total_study_hours 
from study_sessions t1 
    inner join valid_student t2 ON t1.student_id = t2.student_id
    left join students t3 ON t1.student_id = t3.student_id
group by t1.student_id
order by count(distinct t1.subject) desc, sum(t1.hours_studied ) desc