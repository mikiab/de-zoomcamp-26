# Module 6 Homework

## Spark set up

The homework was tested using the Spark distribution provided directly by 
`pyspark`.

Run the following commands to prepare the environment and get the answers to the
questions:

```shell
cd module06/spark
uv sync --locked
```

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute `spark.version`

What's the output?

**Answer:** 

The version provided by `pyspark` is **4.1.1** and can be verified by running 
the [q1](./spark/q1.py) Python script:

```shell
uv run q1.py 
```

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files
that were created (in MB)? Select the answer which most closely matches.

- 6MB
- <mark>25MB</mark>
- 75MB
- 100MB

**Answer:**

The result can be verified by running [q2.py](./spark/q2.py): 

```sh
uv run q2.py
```

The partitioned output is:

```
part-00003-cdcebcee-ef32-433d-981b-3bb9c1472295-c000.snappy.parquet 24.42 MB    
part-00002-cdcebcee-ef32-433d-981b-3bb9c1472295-c000.snappy.parquet 24.42 MB
part-00001-cdcebcee-ef32-433d-981b-3bb9c1472295-c000.snappy.parquet 24.41 MB
part-00000-cdcebcee-ef32-433d-981b-3bb9c1472295-c000.snappy.parquet 24.41 MB
```

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- <mark>162,604</mark>
- 225,768

**Answer:**

The result can be verified by running [q3.py](./spark/q3.py): 

```sh
uv run q3.py
```

```
+------+
| count|
+------+
|162604|
+------+
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- <mark>90.6</mark>
- 134.5

**Answer:**

The result can be verified by running [q4.py](./spark/q4.py):

```sh
uv run q4.py
```

```
+---------+                                                                     
|    hours|
+---------+
|90.633333|
+---------+
```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which
local port?

- 80
- 443
- <mark>4040</mark>
- 8080

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name
of the LEAST frequent pickup location Zone?

- <mark>Governor's Island/Ellis Island/Liberty Island</mark>
- Arden Heights
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any.

**Answer:**

The result can be verified by running [q6.py](./spark/q6.py):

```sh
uv run q6.py
```

```
+---------------------------------------------+---------+                       
|Zone                                         |frequency|
+---------------------------------------------+---------+
|Eltingville/Annadale/Prince's Bay            |1        |
|Arden Heights                                |1        |
|Governor's Island/Ellis Island/Liberty Island|1        |
+---------------------------------------------+---------+
```
