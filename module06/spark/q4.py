from pyspark.sql import SparkSession

from utils import fetch_tripdata


def main():
    pq = fetch_tripdata(taxi_type="yellow", year=2025, month=11)

    spark = SparkSession.builder.master("local[*]").appName("q4").getOrCreate()
    df = spark.read.parquet(str(pq))
    spark.sql(
        """
        SELECT 
          MAX(
            TIMESTAMPDIFF(MINUTE,tpep_pickup_datetime,tpep_dropoff_datetime)/60.0
          ) AS hours
        FROM
          {trips}
        """,
        trips=df,
    ).show()


if __name__ == "__main__":
    main()
