from pyspark.sql import SparkSession

from utils import fetch_tripdata


def main():
    pq = fetch_tripdata(taxi_type="yellow", year=2025, month=11)

    spark = SparkSession.builder.master("local[*]").appName("q3").getOrCreate()
    df = spark.read.parquet(str(pq))
    spark.sql(
        """
        SELECT 
            COUNT(1) AS count
        FROM
            {trips}
        WHERE
            tpep_pickup_datetime >= '2025-11-15'
        AND
            tpep_pickup_datetime < '2025-11-16'
    """,
        trips=df,
    ).show()


if __name__ == "__main__":
    main()
