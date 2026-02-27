"""@bruin
name: ingestion.trips
image: python:3.13
connection: duckdb-default

# Prefer append-only in ingestion; handle duplicates in staging.
materialization:
  type: table
  strategy: append

# Output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
columns:
  - name: service_type
    type: VARCHAR
    description: Taxi type (yellow or green)
  - name: extracted_at
    type: TIMESTAMP
    description: Ingestion time
  - name: vendor_id
    type: INTEGER
    description: Taxi technology provider (1=Creative Mobile Technologies, 2=VeriFone Inc.)
  - name: rate_code_id
    type: INTEGER
    description: Rate code at end of trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)
  - name: pickup_location_id
    type: INTEGER
    description: TLC Taxi Zone where the meter was engaged
  - name: dropoff_location_id
    type: INTEGER
    description: TLC Taxi Zone where the meter was disengaged
  - name: pickup_datetime
    type: TIMESTAMP
    description: Date and time when the meter was engaged
  - name: dropoff_datetime
    type: TIMESTAMP
    description: Date and time when the meter was disengaged
  - name: store_and_fwd_flag
    type: STRING(1)
    description: Flag indicating if trip record was held in vehicle memory (Y/N)
  - name: passenger_count
    type: INTEGER
    description: Number of passengers in the vehicle (driver-entered value)
  - name: trip_type
    type: INTEGER
    description: Code for trip type (1=Street-hail, 2=Dispatch)
  - name: trip_distance
    type: DOUBLE
    description: Trip distance in miles reported by the taximeter
  - name: fare_amount
    type: DOUBLE
    description: Time and distance fare calculated by the meter
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges (rush hour, overnight)
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax automatically triggered based on meter rate
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (credit card tips only, cash tips not included)
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid during the trip
  - name: ehail_fee
    type: DOUBLE
    description: E-hail service fee
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge assessed on hailed trips
  - name: total_amount
    type: DOUBLE
    description: Total amount charged to passengers (does not include cash tips)
  - name: payment_type
    type: INTEGER
    description: Payment method code (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)
  - name: airport_fee
    type: DOUBLE
    description: For pick up only at LaGuardia and John F. Kennedy Airports
  - name: congestion_surcharge
    type: DOUBLE
    description: Total amount collected in trip for NYS congestion surcharge
@bruin"""

import requests
import json
import os
import polars as pl
import duckdb
from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def months(start: date, end: date) -> date:
    delta = relativedelta(end, start)
    for month in range(0, (delta.years * 12) + delta.months + 1):
        yield start + relativedelta(months=month)


def fetch_and_save_trips(
    taxi_type: str, date: date, out_path: str | Path = "data"
) -> Path | None:
    out_path = Path(out_path)
    filename = f"{taxi_type}_tripdata_{date.year}-{date.month:02d}.parquet"
    file_path = out_path / filename
    if not file_path.exists():
        os.makedirs(out_path, exist_ok=True)
        with requests.get(f"{TRIP_URL}/{filename}", stream=True) as trip_data:
            if trip_data.ok:
                with open(file_path, "wb") as trip_file:
                    for data in trip_data.iter_content(chunk_size=8 * 1024 * 1024):
                        trip_file.write(data)
            else:
                return None
    return file_path


def materialize() -> pl.DataFrame:
    start_date = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end_date = date.fromisoformat(os.environ["BRUIN_END_DATE"])
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types")

    trip_files = []
    for taxi_type in taxi_types:
        for date_month in months(start_date, end_date):
            print(f"Fetching {taxi_type} data on {date_month}...", end="")
            trip_file = fetch_and_save_trips(taxi_type, date_month)
            if trip_file:
                print(f"done ({trip_file})")
                trip_files.append(trip_file)
            else:
                print("skipped")

    trips_sql = """
        select
            split_part(parse_filename(filename), '_', 1) as service_type,
            current_timestamp::timestamp as extracted_at,
            cast(VendorID as integer) as vendor_id,
            cast(RatecodeID as integer) as rate_code_id,
            cast(PULocationID as integer) as pickup_location_id,
            cast(DOLocationID as integer) as dropoff_location_id,
            cast(coalesce(lpep_pickup_datetime, tpep_pickup_datetime) as timestamp) as pickup_datetime,
            cast(coalesce(lpep_dropoff_datetime, tpep_dropoff_datetime) as timestamp) as dropoff_datetime,
            cast(store_and_fwd_flag as string(1)) as store_and_fwd_flag,
            cast(passenger_count as integer) as passenger_count,
            cast(trip_type as integer) as trip_type,
            trip_distance,
            cast(payment_type as integer) as payment_type,
            cast(fare_amount as numeric) as fare_amount,
            cast(extra as numeric) as extra,
            cast(mta_tax as numeric) as mta_tax,
            cast(tip_amount as numeric) as tip_amount,
            cast(tolls_amount as numeric) as tolls_amount,
            cast(ehail_fee as numeric) as ehail_fee,
            cast(improvement_surcharge as numeric) as improvement_surcharge,
            cast(total_amount as numeric) as total_amount,
            cast(airport_fee as numeric) as airport_fee,
            cast(congestion_surcharge as numeric) as congestion_surcharge
        from
            taxi
        """

    taxi = duckdb.read_parquet(
        list(map(str, trip_files)), union_by_name=True, filename=True
    )
    trips = taxi.query(virtual_table_name="trips", sql_query=trips_sql)
    return trips.pl()
