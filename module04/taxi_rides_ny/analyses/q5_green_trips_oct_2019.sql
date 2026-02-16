-- Question 5: Green Taxi Trip Counts (October 2019)
select 
    sum(total_monthly_trips) as total_trips 
from 
    {{ ref('fct_monthly_zone_revenue') }}
where 
    service_type = 'Green' 
and 
    revenue_month = '2019-10-01'
