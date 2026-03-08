from pyspark.sql import SparkSession

from utils import fetch_tripdata, fetch_zone_lookup


def main():
    pq = fetch_tripdata(taxi_type="yellow", year=2025, month=11)
    csv = fetch_zone_lookup()

    spark = SparkSession.builder.master("local[*]").appName("q6").getOrCreate()

    df_trips = spark.read.parquet(str(pq))
    df_zones = spark.read.option("header", "true").csv(str(csv))
    df_zones.createOrReplaceTempView("zones")

    spark.sql(
        """
        WITH taxi_pickup_freq AS (
          SELECT 
            PULocationID, COUNT(*) AS freq 
          FROM 
            {trips}
          GROUP BY
            PULocationID
        ), taxi_pickup_freq_rank AS (
          SELECT 
            PULocationID,
            freq,
            RANK() OVER (ORDER BY freq) AS rank
          FROM
            taxi_pickup_freq
        )
        SELECT
          zones.Zone,
          freq as frequency
        FROM
          taxi_pickup_freq_rank
        LEFT JOIN
          zones
        ON
          PULocationID = LocationID
        WHERE
          rank = 1
        """,
        trips=df_trips,
    ).show(truncate=False)


if __name__ == "__main__":
    main()
