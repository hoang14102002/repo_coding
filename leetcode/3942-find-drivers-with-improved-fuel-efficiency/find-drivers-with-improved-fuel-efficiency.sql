# Write your MySQL query statement below

    select t1.driver_id, max(t2.driver_name) as driver_name,
        round(sum(case when month(t1.trip_date) between 1 and 6 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 1 and 6 then 1 else 0 end),2) as first_half_avg,
        round(sum(case when month(t1.trip_date) between 7 and 12 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 7 and 12 then 1 else 0 end),2) as second_half_avg,
        round(
            sum(case when month(t1.trip_date) between 7 and 12 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 7 and 12 then 1 else 0 end)
            -
            sum(case when month(t1.trip_date) between 1 and 6 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 1 and 6 then 1 else 0 end)
            ,2) as efficiency_improvement 

    from trips t1 
        left join drivers t2 on t1.driver_id = t2.driver_id
    group by t1.driver_id
    having round(
            sum(case when month(t1.trip_date) between 7 and 12 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 7 and 12 then 1 else 0 end)
            -
            sum(case when month(t1.trip_date) between 1 and 6 then distance_km/fuel_consumed else 0 end)/
            sum( case when month(t1.trip_date) between 1 and 6 then 1 else 0 end)
            ,2) > 0
    order by efficiency_improvement  desc, driver_name   