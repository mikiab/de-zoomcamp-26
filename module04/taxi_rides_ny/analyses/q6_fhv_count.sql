-- Question 6: Count of records in stg_fhv_tripdata
select count(*) as count from {{ ref('stg_fhv_tripdata') }}
