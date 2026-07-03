# Write your MySQL query statement below

with temp as 
(
    select customer_id, count(distinct transaction_id) as total_trans,
        datediff(max(transaction_date), min(transaction_date)) as period,
        sum(case when transaction_type = 'refund' then 1 else 0 end)/
        count(distinct transaction_id) as refund_rate
    from customer_transactions
    group by customer_id
)
    select customer_id from temp
    where total_trans >= 3 and period >= 30 
        and refund_rate < 0.2