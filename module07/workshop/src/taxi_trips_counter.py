# taxi_trips_counter.py

from kafka import KafkaConsumer
from serializers import ride_deserializer

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))

from src import KAFKA_ADDR, TOPIC


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_ADDR,
        auto_offset_reset="earliest",
        group_id=None,
        enable_auto_commit=False,
        value_deserializer=ride_deserializer,
        consumer_timeout_ms=5000,
    )

    print(f"Listening to {TOPIC}...")

    count = 0
    try:
        for message in consumer:
            ride = message.value
            if ride.trip_distance > 5.0:
                count += 1
    except StopIteration:
        pass

    print(f"There are {count} trips with a distance greater than 5.0 Km")
    consumer.close()


if __name__ == "__main__":
    main()
