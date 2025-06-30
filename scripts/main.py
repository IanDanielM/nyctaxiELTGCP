import asyncio
import os
from datetime import datetime
import httpx
from google.cloud import storage


def handler(request):
    """
    Cloud Function entry point.
    This function is triggered by an HTTP request.
    """
    request_json = request.get_json(silent=True)
    
    print(f"Function triggered by HTTP request.")
    if request_json:
        print(f"Received JSON payload: {request_json}")
    
    asyncio.run(run_data())
    
    print("Function execution finished successfully.")
    return ("OK", 200)


class DataDownloadAndLoad:
    def __init__(self, base_url: str, taxi_type: list, years: list):
        self.base_url = base_url
        self.taxi_type = taxi_type
        self.years = years
        self.download_dir = "/tmp/data/raw"
        self.cloud_storage_client = storage.Client()
        self.bucket_name = 'nyc-taxi-data-101253'
        os.makedirs(self.download_dir, exist_ok=True)

    async def download_file(self, url: str, session: httpx.AsyncClient):
        """Asynchronously downloads a file from a given URL."""
        try:
            response = await session.get(url, timeout=120)
            response.raise_for_status()

            file_name = url.split("/")[-1]
            file_path = os.path.join(self.download_dir, file_name)
            
            with open(file_path, "wb") as f:
                f.write(response.content)
            
            print(f"Successfully downloaded {file_name}")
            return file_path
        except httpx.HTTPStatusError as e:
            print(f"Failed to download {url}. Status code: {e.response.status_code}")
            return None
        except Exception as e:
            print(f"An error occurred while downloading {url}: {e}")
            return None

    async def process_file(self, year: int, month: int, session: httpx.AsyncClient):
        """Processes a single file: downloads and uploads it."""
        month_str = str(month).zfill(2)
        for taxi in self.taxi_type:
            print(f"Processing {taxi} data for {year}-{month_str}")
            url = f"{self.base_url}{taxi}_tripdata_{year}-{month_str}.parquet"
            print(f"Downloading from URL: {url}")
            
            file_path = await self.download_file(url, session)

            if file_path:
                self.upload_to_bucket_partitioned(file_path, taxi, year, month_str)

    async def download_and_load_files(self):
        """
        Creates and runs all download/upload tasks concurrently.
        """
        tasks = []
        current_year = datetime.now().year
        current_month = datetime.now().month

        async with httpx.AsyncClient() as client:
            for year in self.years:
                end_month = current_month if year == current_year else 12
                for month in range(1, end_month + 1):
                    tasks.append(self.process_file(year, month, client))
        
            await asyncio.gather(*tasks)

    def upload_to_bucket_partitioned(self, file_path: str, taxi_type: str, year: int, month: str):
        """
        Uploads a file to a GCS bucket with a Hive-style partitioning structure.
        Removes the local file after a successful upload.
        """
        file_name = os.path.basename(file_path)
        bucket_key = f"taxi_type={taxi_type}/year={year}/month={month}/{file_name}"
        
        try:
            bucket = self.cloud_storage_client.bucket(self.bucket_name)
            blob = bucket.blob(bucket_key)
            blob.upload_from_filename(file_path)
            print(f"Uploaded {file_name} to gs://{self.bucket_name}/{bucket_key}")
            os.remove(file_path)
            print(f"Removed local file: {file_path}")
        except Exception as e:
            print(f"Error uploading {file_name} to bucket: {e}")


async def run_data():
    """Initializes the class and starts the data processing."""
    downloader = DataDownloadAndLoad(
        base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/",
        taxi_type=["yellow", "green"],
        years=list(range(2023, 2025))
    )
    await downloader.download_and_load_files()
