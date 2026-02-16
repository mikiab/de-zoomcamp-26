-- Question 3: Count records in fct_monthly_zone_revenue
select count(*) as count from {{ ref('fct_monthly_zone_revenue') }}
