# Write your MySQL query statement below

    select DATE_FORMAT(trans_date, '%Y-%m') AS 'month', country,
        count(distinct id) as trans_count,
        sum(case when state = 'approved' then 1 else 0 end) as approved_count,
        sum(amount) as trans_total_amount,
        sum(case when state = 'approved' then amount else 0 end) as approved_total_amount 
    from transactions
    group by DATE_FORMAT(trans_date, '%Y-%m'), country