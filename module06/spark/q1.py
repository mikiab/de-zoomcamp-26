from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.master("local[*]").appName("q1").getOrCreate()
    print(f"{spark.version}")


if __name__ == "__main__":
    main()
