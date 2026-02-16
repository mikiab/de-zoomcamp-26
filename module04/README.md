# Module 4 Homework

## Building the Data Warehouse

The solutions provided were tested by using a local dbt setup and *DuckDB* as 
Data Warehouse (the `profiles.yml` file is stored in the project).

### Setup and Data Loading

The commands to build and load data into the Data Warehouse are:
 
```sh
uv sync --locked
uv run ingest.py
```

## Transformations

The *transformations* are performed using the commands:

```
cd taxi_rides_ny/
uv run dbt build --target prod
```

## Analyses

The SQL queries used to answer the questions are stored in the 
[analyses](./taxi_rides_ny/analyses/) directory.

## Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer:** `int_trips_unioned` only because there is no `+` sign to indicate 
upstream or downstream dependencies.

## Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value 6
now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt will fail the test, returning a non-zero exit code.

## Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

**Query:** [analyses/q3_count_zone_revenue.sql](./taxi_rides_ny/analyses/q3_count_zone_revenue.sql)

Execute the command in the `./taxi_rides_ny` folder:

```shell
uv run dbt show -t prod -s q3_count_zone_revenue
```

```
Previewing node 'q3_count_zone_revenue':
| count |
| ----- |
| 12184 |
```

## Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the 
**highest total revenue** (`revenue_monthly_total_amount`) for Green taxi trips 
in 2020.

Which zone had the highest revenue?

**Query:** [analyses/q4_green_2020_best_zone.sql](./taxi_rides_ny/analyses/q4_green_2020_best_zone.sql)

Execute the command in the `./taxi_rides_ny` folder:

```shell
uv run dbt show -t prod -s q4_green_2020_best_zone
```

```
Previewing node 'q4_green_2020_best_zone':
| pickup_zone       | total_revenue |
| ----------------- | ------------- |
| East Harlem North |  1.817.324,25 |
```

## Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of 
trips** (`total_monthly_trips`) for Green taxis in October 2019?

**Query:** [analyses/q5_green_trips_oct_2019.sql](./taxi_rides_ny/analyses/q5_green_trips_oct_2019.sql)

Execute the command in the `./taxi_rides_ny` folder:

```shell
uv run dbt show -t prod -s q5_green_trips_oct_2019
```

```
Previewing node 'q5_green_trips_oct_2019':
| total_trips |
| ----------- |
|      384624 |
```

*Note: Because `fct_monthly_zone_revenue` stores the number of trips per
`pickup_zone`, `revenue_month` and `service_type`, the total number of trips 
is the grand total (sum) of all zones.*

## Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

**Answer:**

1. Data was loaded using the [ingest.py](./ingest.py) script
2. [Staging model](./taxi_rides_ny/models/staging/stg_fhv_tripdata.sql) and [schema](./taxi_rides_ny/models/staging/stg_fhv_tripdata.yml)

What is the count of records in `stg_fhv_tripdata`?

**Query:** [analyses/q6_fhv_count.sql](./taxi_rides_ny/analyses/q6_fhv_count.sql)

```shell
uv run dbt show -t prod -s q6_fhv_count
```

```
Previewing node 'q6_fhv_count':
|    count |
| -------- |
| 43244693 |
```
