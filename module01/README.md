# SQL Questions

## Question 3. Counting short trips

For the trips in November 2025 (`lpep_pickup_datetime` between '2025-11-01' and
'2025-12-01', exclusive of the upper bound), how many trips had a
`trip_distance` of less than or equal to 1 mile?

```sql
SELECT 
    count(1) 
FROM 
    green_tripdata 
WHERE 
    lpep_pickup_datetime >= '2025-11-01' 
AND 
    lpep_pickup_datetime < '2025-12-01' 
AND 
    trip_distance <= 1.0;
```

| count |
|------:|
|   8007|


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips
with `trip_distance` less than 100 miles (to exclude data errors).

```sql
SELECT 
    lpep_pickup_datetime::date
FROM 
    green_tripdata
WHERE 
    trip_distance < 100.0
ORDER BY 
    trip_distance DESC
LIMIT 1;
```

| `lpep_pickup_datetime`|
|----------------------:|
|            2025-11-14 |

## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on
November 18th, 2025?

```sql
SELECT 
    tz."Zone", 
    SUM(gt.total_amount) AS total_amount
FROM 
    taxi_zone_lookup tz
JOIN 
    green_tripdata gt ON tz."LocationID" = gt."PULocationID"
WHERE 
    gt.lpep_pickup_datetime::date = '2025-11-18'
GROUP BY 
    tz."Zone"
ORDER BY 
    total_amount DESC
LIMIT 1;
```

|  `Zone`           | `total_amount`    |
|:------------------|------------------:|
| East Harlem North |  9281.920000000004|


## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November
2025, which was the drop off zone that had the largest tip?

```sql
SELECT 
     tz_pu."Zone" AS pu_zone,
     gt.tip_amount,
     tz_do."Zone" AS do_zone
 FROM
     green_tripdata gt
 JOIN
     taxi_zone_lookup tz_pu ON gt."PULocationID" = tz_pu."LocationID"
 JOIN
     taxi_zone_lookup tz_do ON gt."DOLocationID" = tz_do."LocationID"
 WHERE 
     gt.lpep_pickup_datetime >= '2025-11-01'
 AND
     gt.lpep_pickup_datetime < '2025-12-01'
 AND
     tz_pu."Zone" = 'East Harlem North'
 ORDER BY 
     gt.tip_amount DESC
 LIMIT 1;
```

| `pu_zone`         |  `tip_amount` |   `do_zone`    |
|:------------------|--------------:|:---------------|
| East Harlem North |         81.89 | Yorkville West |

