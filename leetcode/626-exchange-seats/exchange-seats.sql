# Write your MySQL query statement below

SELECT
    CASE
        WHEN id % 2 = 1 AND id = max_id THEN id
        WHEN id % 2 = 1 THEN id + 1
        ELSE id - 1
    END AS id,
    student
FROM (
    SELECT 
        *,
        MAX(id) OVER() AS max_id
    FROM Seat
) t
ORDER BY id;