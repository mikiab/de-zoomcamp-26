# Module 7 Homework

## Kafka, Flink and PostgreSQL Setup

The [workshop folder](./workshop) contains the infrastructure used in the 
[workshop on Flink](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming/workshop) that provides the following containers:

- **Redpanda (Kafka Broker)** on port `9092`
- **Flink**: both the **Job Manager** on port `8081` and the **Task Manager**
- **PostgreSQL** on port `5432`

To set up the environment, run the following commands:

```sh
cd workshop/

docker compose build
docker compose up -d
```

The Flink image provides the connectors for Kafka and PostgreSQL. Furthermore, 
the image integrates `Python`, `uv` and the all dependencies required to build a
PyFlink application and interact with RedPanda and PostgreSQL.

The Flink configuration is customized for a better PyFlink experience.

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Answer: v25.3.9**

The output is:

```
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- <mark>10 seconds</mark>
- 60 seconds
- 120 seconds
- 300 seconds

**Answer:** 
By running the [producer](./workshop/src/taxi_trips_producer.py) script:

```shell
uv run taxi_trips_producer.py
```

It takes about 10 seconds to send all the data to Kafka.

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- <mark>8506</mark>
- 9506

**Answer:**
By running the [counter](./workshop/src/taxi_trips_counter.py) script:

```shell
uv run taxi_trips_counter.py
```

## Question 4. Tumbling window - pickup location
  
Create a Flink job that reads from `green-trips` and uses a 5-minute tumbling 
window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns: `window_start`, 
`PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- <mark>74</mark>
- 75
- 166

**Answer:**

First, create and populate the Kafka topic if it hasn't been done previously.

Then, create a Postgres table to store the aggregated events:

```sql
CREATE TABLE aggregated_trips (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

Execute the Flink job [q4](./workshop/src/job/q4.py) to run the pipeline that
performs the aggregation and writes the result into the `aggregated_trips` 
table:

```shell
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4.py
```

The job progress can be monitored through the Flink dashboard at 
`http://localhost:8081` and by checking the `aggregated_trips` table in the 
`postgres` database.

Execute `pgcli`:

```shell
pgcli postgresql://postgres:postgres@localhost:5432/postgres
```

and run the query to check if all data was successfully written:

```sql
SELECT COUNT(*) FROM aggregated_trips;
```

When ready, execute the query to get the pickup locations that had the most 
trips in the tumbling interval of 5 minutes:

```sql
SELECT 
    PULocationID, num_trips
FROM 
    aggregated_trips
ORDER BY 
    num_trips DESC
LIMIT 
    3;
```

```
+--------------+-----------+
| pulocationid | num_trips |
|--------------+-----------|
| 74           | 15        |
| 74           | 14        |
| 74           | 13        |
+--------------+-----------+
```

## Question 5. Session window - longest streak

Create another Flink job that uses a **session window** with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time with a 
5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID` with the 
longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- <mark>81</mark>

**Answer:**

Create and populate the Kafka topic if it hasn't been done previously.

Define the Postgres table to store the Flink job aggregations:

```sql
CREATE TABLE aggregated_trips_by_session (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
```

Execute the Flink job [q5](./workshop/src/job/q5.py):

```shell
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5.py
```

Wait until the `aggregated_trips_by_session` is filled by Flink.

The `SESSION` function groups elements by sessions of activity. In contrast to
`TUMBLE` windows, **session windows** do not have a fixed *start* and *end time*
but instead close the window when a *gap of inactivity* occurs. For this reason,
the table stores both the `window_start` and `window_end` timestamps alongside
the `PULocationID` column to count the number of trips in a window session.

Execute the following query to get the answer to the question:

```sql
SELECT 
    * 
FROM 
    aggregated_trips_by_session 
ORDER BY 
    num_trips DESC 
LIMIT 
    1;
```

```
+---------------------+---------------------+--------------+-----------+
| window_start        | window_end          | pulocationid | num_trips |
|---------------------+---------------------+--------------+-----------|
| 2025-10-08 06:46:14 | 2025-10-08 08:27:40 | 74           | 81        |
+---------------------+---------------------+--------------+-----------+
```

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the total 
`tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- <mark>2025-10-16 18:00:00</mark>
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

**Answer:**

Create a new Postgres table to store the aggregated data using the following SQL 
statement:

```sql
CREATE TABLE aggregated_tip_amount (
    window_start TIMESTAMP,
    tip_amount FLOAT,
    PRIMARY KEY (window_start)
);
```

Execute the Flink job [q6](./workshop/src/job/q6.py):

```shell
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6.py
```

Run the following query to get the date with the highest tip amount:

```sql
SELECT 
    MAX(tip_amount), 
    window_start 
FROM 
    aggregated_tip_amount 
GROUP BY 
    window_start 
ORDER BY 
    1 DESC 
LIMIT 
    1;  
```

```
+-------------------+---------------------+
| max               | window_start        |
|-------------------+---------------------|
| 524.9599999999998 | 2025-10-16 18:00:00 |
+-------------------+---------------------+
```
