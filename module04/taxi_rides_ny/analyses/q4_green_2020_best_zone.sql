-- Question 4: Best Performing Zone for Green Taxis (2020)
with cte as (
    select 
        pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue 
    from 
        {{ ref('fct_monthly_zone_revenue') }}
    where 
        service_type = 'Green' 
    and 
        year(revenue_month) = 2020 
    group by 
        pickup_zone
    order bY
        total_revenue desC
    limit 1
)

select * from cte
