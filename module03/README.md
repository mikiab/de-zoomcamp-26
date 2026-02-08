# Module 3 Homework

The module uses a slightly modified script to load data in GCP by setting a 
longer upload timeout (5 minutes) and a check to avoid re-downloading data if it 
was already downloaded.

## Prepare the environment

```sh
uv sync --locked
```

Load data in GCP bucket:

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp/credentials.json
uv run load_yellow_taxi_data.py
```

## BigQuery Setup

Create an **external table** using the Yellow Taxi Trip Records.

```sql
CREATE OR REPLACE EXTERNAL TABLE `gcp-taxi.taxi_ds.ext_yellow_taxi_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-data-c1852b1c-025b-4f2a-abac-dec70608b3d5/*']
);
```

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records
(do not partition or cluster this table).

```sql
CREATE OR REPLACE TABLE `gcp-taxi.taxi_ds.yellow_taxi_trips_raw`
AS
SELECT * FROM `gcp-taxi.taxi_ds.ext_yellow_taxi_trips`;
```

Both tables are built by using a **schema-on-read** approach.

## Question 1

What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT
  COUNT(*) AS count
FROM
  `taxi_ds.yellow_taxi_trips_raw`
```

|  `count`  |
|----------:|
|  20332093 |

## Question 2

Write a query to count the distinct number of `PULocationIDs` for the entire
dataset on both the tables.

What is the **estimated amount** of data that will be read when this query is
executed on the External Table and the Table?

```sql
SELECT
  COUNT(DISTINCT t.PULocationID)
FROM
  `taxi_ds.yellow_taxi_trips_raw` t
```

In the BigQuery Console:

```
This query will process 155.12 MB when run.
```

```sql
SELECT
  COUNT(DISTINCT t.PULocationID)
FROM
  `taxi_ds.ext_yellow_taxi_trips` t
```

In the BigQuery Console:

```
This query will process 0 MB when run.
```

## Question 3

Write a query to retrieve the `PULocationID` from the table (not the external
table) in BigQuery. Now write a query to retrieve the `PULocationID` and
`DOLocationID` on the same table.

Why is the estimated number of bytes different?

The first query is estimated to process `155.12 MB`

```sql
SELECT
  t.PULocationID
FROM
  `taxi_ds.yellow_taxi_trips_raw` t
```

The second query will process `310.24 MB` because it will scan all data for 
both columns, doubling the processed bytes.

```sql
SELECT
  t.PULocationID, t.DOLocationID
FROM
  `taxi_ds.yellow_taxi_trips_raw` t
```

BigQuery is a columnar database, and it only scans the specific columns
requested in the query. Querying two columns (`PULocationID, DOLocationID`)
requires reading more data than querying one column (`PULocationID`), leading to
a higher estimated number of bytes processed.

## Question 4

How many records have a `fare_amount` of 0?

```sql
SELECT
  COUNT(1) as count
FROM
  `taxi_ds.yellow_taxi_trips_raw` t
WHERE
  t.fare_amount = 0;
```

|  `count`  |
|-----------|
|      8333 |

## Question 5

What is the best strategy to make an optimized table in BigQuery if your query
will always filter based on `tpep_dropoff_datetime` and order the results by
`VendorID` (Create a new table with this strategy).

The best strategy is to combine partitioning and clustering: partitioning by 
`tpep_dropoff_datetime` allows *partition pruning*; clustering on `VendorID` 
retrieves the same values faster because the data is co-located.

```sql
CREATE OR REPLACE TABLE `gcp-taxi.taxi_ds.yellow_taxi_trips`
  PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID
AS
SELECT * FROM `gcp-taxi.taxi_ds.yellow_taxi_trips_raw`;
```

> Note: the question mentions `tpep_dropoff_datetime` which contains timestamps, 
> but it should refer to the date part of `tpep_dropoff_datetime` because the 
> next question asks to filter the new partioned table by specific date range.

## Question 6

Write a query to retrieve the distinct `VendorIDs` between
`tpep_dropoff_datetime` *2024-03-01* and *2024-03-15 (inclusive)*.

Use the materialized table you created earlier in your from clause and note the
estimated bytes. Now change the table in the from clause to the partitioned
table you created for question 5 and note the estimated bytes processed. What
are these values?

Choose the answer which most closely matches.

For the *non-partitioned table*, the query:

```sql
SELECT DISTINCT t.VendorID
FROM
  `gcp-taxi.taxi_ds.yellow_taxi_trips_raw` t
WHERE
  DATE(t.tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

In the BigQuery Console:

```
This query will process 310.24 MB when run.
```

In contrast, the query on the *partitioned table*:

```sql
SELECT DISTINCT t.VendorID
FROM
  `gcp-taxi.taxi_ds.yellow_taxi_trips` t
WHERE
  DATE(t.tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

In the BigQuery Console: 

```
This query will process 26.84 MB when run.
```

## Question 7

Where is the data stored in the External Table you created?

The data is stored into a GCP Bucket as indicated by the `uris` option `gs://`.

## Question 8

It is best practice in Big Query to always cluster your data.

The answer is false because it depends on how that table will be queried.

## Question 9

Write a `SELECT COUNT(*)` query from the materialized table you created. How 
many bytes does it estimate will be read? Why?

The query on the partitioning and clustering table:

```sql
SELECT
  COUNT(*) as count
FROM
  `taxi_ds.yellow_taxi_trips` t
```

It estimates 0 bytes processed because BigQuery extracts the number of records
from the **Metadata** without scanning data.

The result is the same even if the query is executed on the non-partitioned and 
non-clustered table.

However, if some filter is applied to the query, in this case BigQuery will 
perform a scan and there will be a difference in performance and costs if the
table is partitioned/clustered or not.
