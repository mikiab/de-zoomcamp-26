#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

TRIP_DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
)
ZONE_DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"


def extract_from_sources():
    green_tripdata = pd.read_parquet(TRIP_DATA_URL)
    taxi_zone_lookup = pd.read_csv(
        ZONE_DATA_URL,
        dtype={
            "LocationID": "int64",
            "Borough": "string",
            "Zone": "string",
            "service_zone": "string",
        },
    )
    return taxi_zone_lookup, green_tripdata


def load_into_db(taxi_zone_lookup, green_tripdata, **pg_conf):
    pg_url = URL.create(
        drivername="postgresql+psycopg2",
        host=pg_conf["pg_host"],
        port=pg_conf["pg_port"],
        database=pg_conf["pg_db"],
        username=pg_conf["pg_user"],
        password=pg_conf["pg_pass"],
    )
    engine = create_engine(pg_url)
    taxi_zone_lookup.to_sql(name="taxi_zone_lookup", con=engine, if_exists="replace")
    green_tripdata.to_sql(
        name="green_tripdata",
        con=engine,
        if_exists="replace",
        method="multi",  # PG supports multiple rows in a single INSERT
        chunksize=5000,  # How many rows in a single INSERT
    )


@click.command()
@click.option("--pg-user", default="postgres", help="PostgreSQL user")
@click.option("--pg-pass", default="postgres", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5433, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
def main(**pg_conf):
    datasets = extract_from_sources()
    load_into_db(*datasets, **pg_conf)


if __name__ == "__main__":
    main()
