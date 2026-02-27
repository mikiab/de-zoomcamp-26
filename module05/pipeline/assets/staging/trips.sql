/* @bruin

name: staging.trips
type: duckdb.sql

# Declare dependencies so `bruin run ... --downstream` and lineage work.
# Lineage shows how the connections between these dependencies
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_id
    type: BIGINT
    description: Surrogate key
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: service_type
    type: VARCHAR
    description: Taxi type (yellow or green)
  - name: extracted_at
    type: TIMESTAMP
    description: Ingestion time
  - name: vendor_id
    type: INTEGER
    description: Taxi technology provider (1=Creative Mobile Technologies, 2=VeriFone Inc.)
    checks:
      - name: not_null
  - name: rate_code_id
    type: INTEGER
    description: Rate code at end of trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)
  - name: pickup_location_id
    type: INTEGER
    description: TLC Taxi Zone where the meter was engaged
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: INTEGER
    description: TLC Taxi Zone where the meter was disengaged
    checks:
      - name: not_null
  - name: pickup_datetime
    type: TIMESTAMP
    description: Date and time when the meter was engaged
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: TIMESTAMP
    description: Date and time when the meter was disengaged
    checks:
      - name: not_null
  - name: store_and_fwd_flag
    type: STRING(1)
    description: Flag indicating if trip record was held in vehicle memory (Y/N)
  - name: passenger_count
    type: INTEGER
    description: Number of passengers in the vehicle (driver-entered value)
  - name: trip_type
    type: INTEGER
    description: Code for trip type (1=Street-hail, 2=Dispatch)
  - name: trip_distance
    type: DOUBLE
    description: Trip distance in miles reported by the taximeter
  - name: fare_amount
    type: DOUBLE
    description: Time and distance fare calculated by the meter
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges (rush hour, overnight)
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax automatically triggered based on meter rate
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (credit card tips only, cash tips not included)
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid during the trip
  - name: ehail_fee
    type: DOUBLE
    description: E-hail service fee
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge assessed on hailed trips
  - name: total_amount
    type: DOUBLE
    description: Total amount charged to passengers (does not include cash tips)
  - name: payment_type
    type: INTEGER
    description: Payment method code (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)
  - name: airport_fee
    type: DOUBLE
    description: For pick up only at LaGuardia and John F. Kennedy Airports
  - name: congestion_surcharge
    type: DOUBLE
    description: Total amount collected in trip for NYS congestion surcharge

# Define a custom quality check query that validates a staging invariant 
# (uniqueness, ranges, etc.). For example, row count is greater than zero, 
custom_checks:
  - name: table_is_not_empty
    description: Ensure the table is not empty
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1
  - name: dropoff_greater_than_pickup_datetime
    description: Ensure dropoff datetime is strictly greater than pickup datetime
    query: |
      SELECT BOOL_AND(dropoff_datetime > pickup_datetime) FROM staging.trips
    value: 1
  - name: trip_duration_is_valid
    description: Ensure trip duration (dropoff-pickup) is realistic
    query: |
      SELECT 
        COUNT(*) 
      FROM 
        staging.trips 
      WHERE 
        DATE_DIFF('HOUR', pickup_datetime, dropoff_datetime) > 24
    value: 0
  - name: positive_amounts
    description: Ensure positive amounts
    query: |
      SELECT
        COUNT(*)
      FROM
        staging.trips 
      WHERE
        COALESCE(total_amount, 0) >= 0 
      AND
        COALESCE(fare_amount) >= 0
      AND
        COALESCE(extra) >= 0
      AND
        COALESCE(mta_tax) >= 0
      AND
        COALESCE(tip_amount) >= 0
      AND
        COALESCE(tolls_amount) >= 0
      AND
        COALESCE(ehail_fee) >= 0
      AND
        COALESCE(improvement_surcharge) >= 0
      AND
        COALESCE(total_amount) >= 0
      AND
        COALESCE(airport_fee) >= 0
      AND
        COALESCE(congestion_surcharge) >= 0
    value: 0
@bruin */

WITH cleansed AS (
    SELECT 
        * REPLACE (
            @(fare_amount) AS fare_amount,
            @(extra) AS extra,
            @(mta_tax) AS mta_tax,
            @(tip_amount) AS tip_amount,
            @(tolls_amount) AS tolls_amount,
            @(ehail_fee) AS ehail_fee,
            @(improvement_surcharge) AS improvement_surcharge,
            @(total_amount) AS total_amount,
            @(payment_type) AS payment_type,
            @(airport_fee) AS airport_fee,
            @(congestion_surcharge) AS congestion_surcharge
        )
    FROM 
        ingestion.trips 
    WHERE 
        vendor_id IS NOT NULL 
    AND
        pickup_location_id IS NOT NULL
    AND
        dropoff_location_id IS NOT NULL
    AND
        dropoff_datetime > pickup_datetime
    AND
        DATE_DIFF('HOUR', pickup_datetime, dropoff_datetime) <= 24
), 
enriched AS (
    SELECT
        HASH(CONCAT_WS('||',
            t.service_type,
            t.pickup_datetime,
            t.pickup_location_id,
            t.dropoff_location_id,
            t.vendor_id
        )) AS trip_id,
        t.* REPLACE(COALESCE(t.payment_type, 5) AS payment_type),
        p.payment_type_name AS payment_description
    FROM
        cleansed t
    LEFT JOIN 
        ingestion.payment_lookup p 
    ON 
        COALESCE(t.payment_type, 5) = p.payment_type_id
), 
deduplicated AS (
    SELECT 
        * 
    FROM 
        enriched 
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, pickup_datetime, pickup_location_id, service_type
            ORDER BY dropoff_datetime
        ) = 1
)

SELECT 
    *
FROM 
    deduplicated
WHERE 
    pickup_datetime >= '{{ start_datetime }}'
AND 
    pickup_datetime < '{{ end_datetime }}'
