# taxi_trips_counter.py

from kafka import KafkaProducer
import pandas as pd
from time import time

from models import Ride
from serializers import ride_serializer

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))

from src import KAFKA_ADDR, TOPIC, TAXI_URL


def fetch_taxi_data(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    df = pd.read_parquet(
        f"{TAXI_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet",
        columns=Ride.field_names(),
    )
    return df


def main():
    df = fetch_taxi_data("green", 2025, 10)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_ADDR,
        value_serializer=ride_serializer,
    )

    t0 = time()
    for _, row in df.iterrows():
        ride = Ride.from_row(row)
        producer.send(TOPIC, value=ride)

    producer.flush()
    t1 = time()
    print(f"took {(t1 - t0):.2f} seconds")
    producer.close()


if __name__ == "__main__":
    main()
