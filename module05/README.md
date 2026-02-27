# Module 5 Homework

## NYC Taxi Pipeline Setup

Create a `./.bruin.yml` file in the `module05/` folder:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: nyc-taxi.duckdb
```

To run the pipeline and build the database execute:

```sh
cd module05/

bruin run ./pipeline/pipeline.yml \
    --config-file=./.bruin.yml \
    --start-date 2022-03-01 --end-date 2022-03-01 \
    --full-refresh \
    --workers=1
```

Yellow and green taxi data are ingested by running the Python asset 
[`trip.py`](./pipeline/assets/ingestion/trips.py). 
The script uses the `duckdb` library to define the `ingestion.trips` table 
layout and merge data from both taxi types.
Finally, the merged table `ingestion.trips` is returned as a Polars dataframe 
and materialized by Bruin in the database.

During this stage, a 
[seed asset](./pipeline/assets/ingestion/payment_lookup.asset.yml) loads static
payment methods as a CSV file.

The `ingestion.trips` asset sets an *append-only materialization strategy* when
the pipeline is run which can result in duplicate data for the period specified 
by the `--start-date` and `--end-date` flags.

The next step of the pipeline creates the `staging.trips` table by using a 
[SQL asset](./pipeline/assets/staging/trips.sql) which cleans and deduplicates 
the `ingestion.trips` data and enriches it with the payment data.

The asset performs custom quality checks on the `staging.trips` table to ensure:
- The outcome table is not empty.
- The *dropoff datetime* is greater than the *pickup datetime*.
- Trip duration is realistic and is contained within a single day.
- The amount fields are always positive.

The staging asset uses a *time-interval incremental materialization strategy* 
based on the value of the field `pickup_datetime`. When the pipeline runs, Bruin
first deletes all records within the time window specified by `--start-date` and 
`--end-date` flags and then inserts new data by using the query defined in 
the asset.

The last stage builds a report stored in the table
[`reports.trips_reports`](./pipeline/assets/reports/trips_report.sql) by using 
staging data. 

The metrics computed by the asset are the *total number of rides* and the 
*revenue* aggregated by the dimensions of date, taxi type and payment method.

Data quality is ensured by Bruin checking for *primary keys* and *non negative*
metric values.

The materialization strategy used by this asset must be coherent with what was 
set in the staging asset.

## Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- <mark>`.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`</mark>
- `pipeline.yml` and `assets/` only

## Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month
based on `pickup_datetime`. Which incremental strategy is best for processing a
specific interval period by deleting and inserting data for that time period?

- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- <mark>`time_interval` - incremental based on a time column</mark>
- `view` - create a virtual table only

## Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- <mark>`bruin run --var 'taxi_types=["yellow"]'`</mark>
- `bruin run --set taxi_types=["yellow"]`

## Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all
downstream assets. Which command should you use?

- `bruin run ingestion.trips --all`
- <mark>`bruin run ingestion/trips.py --downstream`</mark>
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

## Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has
`NULL` values. Which quality check should you add to your asset definition?

- `name: unique`
- <mark>`name: not_null`</mark>
- `name: positive`
- `name: accepted_values, value: [not_null]`

## Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph
between assets. Which Bruin command should you use?

- `bruin graph`
- `bruin dependencies`
- <mark>`bruin lineage`</mark>
- `bruin show`

## Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database.
What flag should you use to ensure tables are created from scratch?

- `--create`
- `--init`
- <mark>`--full-refresh`</mark>
- `--truncate`
