# serializers.py

import dataclasses
import json
from datetime import datetime
from models import Ride


def ride_serializer(ride: Ride) -> bytes:
    DT_FMT = {"sep": " ", "timespec": "seconds"}
    ride_dict = dataclasses.asdict(ride)
    ride_dict["lpep_pickup_datetime"] = ride_dict["lpep_pickup_datetime"].isoformat(
        **DT_FMT
    )
    ride_dict["lpep_dropoff_datetime"] = ride_dict["lpep_dropoff_datetime"].isoformat(
        **DT_FMT
    )
    json_str = json.dumps(ride_dict)
    return json_str.encode("utf-8")


def ride_deserializer(data: bytes) -> Ride:
    json_str = data.decode("utf-8")
    ride_dict = json.loads(json_str)
    ride_dict["lpep_pickup_datetime"] = datetime.fromisoformat(
        ride_dict["lpep_pickup_datetime"]
    )
    ride_dict["lpep_dropoff_datetime"] = datetime.fromisoformat(
        ride_dict["lpep_dropoff_datetime"]
    )
    return Ride(**ride_dict)
