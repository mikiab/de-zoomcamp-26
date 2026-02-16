import duckdb
import requests
from pathlib import Path

TAXI_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
FHV_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DB_PATH = Path("data") / "taxi_rides_ny.duckdb"


def cvs_to_parquet(csv_filepath, parquet_filepath):
    print(f"Converting {csv_filepath.name} to Parquet...")
    con = duckdb.connect()
    con.execute(f"""
       COPY (SELECT * FROM read_csv_auto('{csv_filepath}'))
       TO '{parquet_filepath}' (FORMAT PARQUET)
    """)
    con.close()
    # Remove the CSV.gz file to save space
    csv_filepath.unlink()
    print(f"Completed {parquet_filepath.name}")


def download_and_convert_files(url, out_dir, file_prefix, years):
    out_dir.mkdir(exist_ok=True, parents=True)
    for year in years:
        for month in range(1, 13):
            filename = f"{file_prefix}_{year}-{month:02d}"

            parquet_filepath = out_dir / f"{filename}.parquet"
            if parquet_filepath.exists():
                print(f"Skipping {parquet_filepath.name} (already exists)")
                continue

            # Download CSV.gz file
            csv_filepath = out_dir / f"{filename}.csv.gz"
            response = requests.get(f"{url}/{csv_filepath.name}", stream=True)
            response.raise_for_status()

            with open(csv_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            cvs_to_parquet(csv_filepath, parquet_filepath)


def download_taxi_files(taxi_type):
    data_dir = Path("data") / taxi_type
    download_and_convert_files(
        f"{TAXI_URL}/{taxi_type}", data_dir, f"{taxi_type}_tripdata", [2019, 2020]
    )


def download_fhv_files():
    data_dir = Path("data") / "fhv"
    download_and_convert_files(f"{FHV_URL}/fhv", data_dir, "fhv_tripdata", [2019])


def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if "data/" not in content:
        with open(gitignore_path, "a") as f:
            f.write(
                "\n# Data directory\ndata/\n"
                if content
                else "# Data directory\ndata/\n"
            )


def extract_from_sources():
    for taxi_type in ["yellow", "green"]:
        download_taxi_files(taxi_type)
    download_fhv_files()


def load_into_db():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    for taxi_type in ["yellow", "green"]:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
        """)

    con.execute("""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)

    con.close()


def run_pipeline():
    extract_from_sources()
    load_into_db()


if __name__ == "__main__":
    # Exclude data directory
    update_gitignore()
    run_pipeline()
