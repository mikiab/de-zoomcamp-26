from pyspark.sql import SparkSession
from pathlib import Path
import os

from utils import DATA_DIR, fetch_tripdata


def print_parquet_partitions(path: Path | str):
    pq_path = Path(path)
    if pq_path.exists():
        with os.scandir(pq_path) as it:
            for entry in it:
                if entry.name.endswith("parquet"):
                    print(f"{entry.name} {entry.stat().st_size / 1024 / 1024:.2f} MB")


def main():
    spark = SparkSession.builder.master("local[*]").appName("q2").getOrCreate()
    pq = fetch_tripdata(taxi_type="yellow", year=2025, month=11, out_path=DATA_DIR)
    df = spark.read.parquet(str(pq))
    df.repartition(4).write.parquet(f"{DATA_DIR}/partitioned", mode="overwrite")
    print_parquet_partitions(f"{DATA_DIR}/partitioned")


if __name__ == "__main__":
    main()
