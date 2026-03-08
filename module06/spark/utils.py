from pathlib import Path
import requests
import os

TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc"

DATA_DIR = "../data"


def fetch_tripdata(
    taxi_type: str, year: int, month: int, out_path: Path | str = DATA_DIR
) -> Path:
    out_path = Path(out_path)
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    file_path = out_path / filename
    if not file_path.exists():
        os.makedirs(out_path, exist_ok=True)
        print(f"Fetching {filename}...", end="")
        with requests.get(f"{TRIP_DATA_URL}/{filename}", stream=True) as trip_data:
            trip_data.raise_for_status()
            with open(file_path, "wb") as trip_file:
                for data in trip_data.iter_content(chunk_size=8 * 1024 * 1024):
                    trip_file.write(data)
        print("done")
    return file_path


def fetch_zone_lookup(out_path: Path | str = DATA_DIR) -> Path:
    out_path = Path(out_path)
    filename = "taxi_zone_lookup.csv"
    file_path = out_path / filename
    if not file_path.exists():
        os.makedirs(out_path, exist_ok=True)
        print(f"Fetching {filename}...", end="")
        with requests.get(f"{ZONE_URL}/{filename}") as zone_data:
            zone_data.raise_for_status()
            with open(file_path, "w") as zone_file:
                zone_file.write(zone_data.text)
        print("done")
    return file_path
