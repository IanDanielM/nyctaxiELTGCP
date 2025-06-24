import asyncio
import os
from datetime import datetime

import httpx
from google.cloud import storage
import uuid


class DataDownloadAndLoad:
    def __init__(self, base_url: str, taxi_type: list, years: list):
        self.base_url = base_url
        self.taxi_type = taxi_type
        self.years = years
        self.download_dir = "data/raw"
        self.cloud_storage_client = storage.Client()
        self.bucket_name = 'nyc-taxi-data-101253'
        os.makedirs(self.download_dir, exist_ok=True)


    def _create_storage_folder(self):
        """
        Create a folder in Google Cloud Storage with a unique name.
        """
        bucket = self.cloud_storage_client.bucket(self.bucket_name)
        folder_name = f"raw_data"
        blob = bucket.blob(folder_name + "/")
        blob.upload_from_string('')
        print(f"Created folder: {folder_name} in bucket: {self.bucket_name}")
        return folder_name
        
    async def download_file(self, url: str):
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url)
            print(response.status_code)
            if response.status_code == 200:
                file_name = url.split("/")[-1]
                file_path = os.path.join(self.download_dir, file_name)
                with open(file_path, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {file_name}")
                return file_path
            else:
                print(f"Failed to download {url}")
                return None

    async def process_file(self, year: int, month: int):
        month_str = str(month).zfill(2)
        for taxi_type in self.taxi_type:
            print(f"Processing {taxi_type} data for {year}-{month_str}")
            url = f"{self.base_url}{taxi_type}_tripdata_{year}-{month_str}.parquet"
            print(f"Downloading from URL: {url}")
            file_path = await self.download_file(url)

            # If download was successful, upload to bucket with partitioning
            if file_path:
                self.upload_to_bucket_partitioned(
                    file_path, taxi_type, year, month_str)

    async def download_and_load_files(self):
        tasks = []
        current_year = datetime.now().year
        current_month = datetime.now().month
        for year in self.years:
            print(year)
            for month in range(1, 13):
                if year == current_year and month > current_month:
                    continue
                tasks.append(self.process_file(year, month))
        await asyncio.gather(*tasks)

    def upload_to_bucket(self, file_path: str):
        file_name = os.path.basename(file_path)
        bucket_key = f"raw_data/{file_name}"
        self.self.cloud_storage_client.bucket(self.bucket_name).blob(bucket_key).upload_from_filename(file_path)
        print(f"Uploaded {file_name} to bucket {self.bucket_name} with key {bucket_key}")

    def upload_to_bucket_partitioned(self, file_path: str, taxi_type: str, year: int, month: str):
        """
        Upload file to bucket with partitioning structure:
        """
        file_name = os.path.basename(file_path)
        bucket_key = f"taxi_type={taxi_type}/year={year}/month={month}/{file_name}"

        try:
            self.cloud_storage_client.bucket(self.bucket_name).blob(bucket_key).upload_from_filename(file_path)
            print(f"Uploaded {file_name} to bucket {self.bucket_name} with partitioned key: {bucket_key}")

            os.remove(file_path)
            print(f"Removed local file: {file_path}")
        except Exception as e:
            print(f"Error uploading {file_name} to bucket: {e}")


async def main():
    downloader = DataDownloadAndLoad(base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/",
                                     taxi_type=["yellow", "green"],
                                     years=list(range(2020, 2025)))  # Years 2020-2025
    await downloader.download_and_load_files()

if __name__ == "__main__":
    asyncio.run(main())
