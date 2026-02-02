# Module 2 Homework

The homework solutions utilize [this](./flows/zoomcamp.gcp_taxi_scheduled.yaml) 
GCP scheduled YAML file. All queries were run in Google BigQuery.

*Note: For the row count queries, I filtered by filename to ensure I counted 
exactly the rows from the requested files, rather than partitioning by date. 
This causes a full table scan and would be inefficient in a production 
environment, but it was necessary to handle the dirty data (incorrect years) 
present in the source files.*

## Question 1

Within the execution for Yellow Taxi data for the year 2020 and month 12: 
what is the uncompressed file size (i.e. the output file 
`yellow_tripdata_2020-12.csv` of the extract task)?

One solution is to create a `get_csv_size` task that computes the CSV file size 
using the `io.kestra.plugin.core.storage.Size` plugin and logs the result by 
reading the output value. Both tasks only run during the 2020-12 backfill.

```yaml
tasks:
  - id: get_csv_size
    type: io.kestra.plugin.core.storage.Size
    runIf: "{{ trigger.date | date('yyyy-MM') == '2020-12'}}"
    uri: "{{render(vars.data)}}"
  
  - id: log_csv_size
    type: io.kestra.plugin.core.log.Log
    runIf: "{{ trigger.date | date('yyyy-MM') == '2020-12'}}"
    message: |
      {{render(vars.file)}} size: {{outputs.get_csv_size.size/1024/1024}} MiB
```

## Question 2 

What is the rendered value of the variable file when the inputs taxi is set to 
green, year is set to 2020, and month is set to 04 during execution?

Answer: `green_tripdata_2020-04.csv`

## Question 3

How many rows are there for the Yellow Taxi data for all CSV files in the year
2020?

```sql
SELECT 
    COUNT(1) AS count
FROM 
    `gcp-taxi.taxi_ds.yellow_tripdata` 
WHERE 
    STRPOS(filename, '2020') > 0
```

| `count` |
|--------:|
| 24648499|

## Question 4

How many rows are there for the Green Taxi data for all CSV files in the year 
2020?

```sql
SELECT 
    COUNT(1) AS count
FROM 
    `gcp-taxi.taxi_ds.green_tripdata` 
WHERE 
    STRPOS(filename, '2020') > 0
```

| `count` |
|--------:|
|  1734051|

## Question 5

How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

```sql
SELECT 
    COUNT(1) AS count
FROM 
    `gcp-taxi.taxi_ds.yellow_tripdata` 
WHERE 
    STRPOS(filename, '2021-03') > 0
```

| `count` |
|--------:|
|  1925152|

## Question 6

How would you configure the timezone to New York in a Schedule trigger?

```diff
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
+   timezone: "America/New_York"
    cron: "0 9 1 * *"
    inputs:
      taxi: green
```
