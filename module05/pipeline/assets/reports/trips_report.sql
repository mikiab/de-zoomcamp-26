/* @bruin

name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: date
    type: DATE
    description: Service day
    primary_key: true
  - name: service_type
    type: STRING
    description: Service type (yellow or green taxi)
    primary_key: true
  - name: payment_type
    description: Payment type ID
    type: INTEGER
    primary_key: true
  - name: payment_description
    type: STRING
    description: |
        Payment type:
          - 0: Flex Fare trip
          - 1: Credit card
          - 2: Cash
          - 3: No charge
          - 4: Dispute
          - 5: Unknown
          - 6: Voided trip
  - name: total_rides
    type: BIGINT
    description: Total number of rides by date, taxi type and payment method
    checks:
      - name: non_negative
  - name: revenue
    type: NUMERIC
    description: Total revenue by date, taxi type and payment method
    checks:
      - name: non_negative
 
@bruin */

SELECT 
    DATE_TRUNC('month', pickup_datetime)::DATE AS date,
    service_type,
    payment_type,
    payment_description,
    COUNT(*) AS total_rides,
    ROUND(SUM(total_amount), 2) AS revenue
FROM 
    staging.trips
WHERE 
    pickup_datetime >= '{{ start_datetime }}'
AND 
    pickup_datetime < '{{ end_datetime }}'
GROUP BY
    1, 2, 3, 4
ORDER BY
    date
