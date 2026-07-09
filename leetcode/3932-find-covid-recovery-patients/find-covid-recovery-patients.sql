# Write your MySQL query statement below

with temp as
(
    select patient_id 
    from covid_tests
    group by patient_id
    having count(distinct result) > 1
),
min_positive as
(
    select patient_id, min(test_date) as min_positive
    from covid_tests
    where result = 'Positive'
    group by patient_id
),
min_negative as
(
    select a.patient_id, max(c.patient_name) as patient_name, max(c.age) as age,
        datediff(min(a.test_date),min(b.min_positive)) as recovery_time 
    from covid_tests a 
        left join min_positive b on a.patient_id = b.patient_id
        left join patients c on a.patient_id = c.patient_id
    where a.result = 'Negative' and a.test_date >= b.min_positive 
    group by a.patient_id
)
select * from min_negative 
-- where patient_id = 4
order by recovery_time, patient_name