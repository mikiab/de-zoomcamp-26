# models.py

import dataclasses
import datetime
import pandas as pd
import typing
import math


@dataclasses.dataclass
class Ride:
    lpep_pickup_datetime: datetime.datetime
    lpep_dropoff_datetime: datetime.datetime
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

    @classmethod
    def field_names(cls) -> list[str]:
        return [f.name for f in dataclasses.fields(cls)]

    @classmethod
    def from_row(cls, row: pd.Series):
        hints = typing.get_type_hints(cls)
        DEFAULTS = {int: 0, float: 0.0, str: ""}

        row_dict = {}
        for f in dataclasses.fields(cls):
            val = row[f.name]
            actual_type = hints[f.name]

            if actual_type is datetime.datetime:
                row_dict[f.name] = val.to_pydatetime()  # type: ignore
            elif isinstance(val, float) and math.isnan(val):
                row_dict[f.name] = DEFAULTS.get(actual_type, None)
            else:
                row_dict[f.name] = actual_type(val)

        return cls(**row_dict)
