with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationID as integer) as pickup_location_id,
        cast(dolocationID as integer) as dropoff_location_id,
        cast(coalesce(affiliated_base_number, dispatching_base_num) as string) as affiliated_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as integer) as shared_ride_flag
    from source
    where dispatching_base_num is not null
)

select * from renamed
