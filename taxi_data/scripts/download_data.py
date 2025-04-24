import asyncio
import os
from datetime import datetime

import httpx
import boto3


class DataDownloadAndLoad:
    def __init__(self, base_url: str, taxi_type: list, years: list):
        self.base_url = base_url
        self.taxi_type = taxi_type
        self.years = years
        self.download_dir = "data/raw"
        self.s3_client = boto3.client('s3')
        self.s3_bucket = "nyc-taxi-data-ed0a62da"
        os.makedirs(self.download_dir, exist_ok=True)

    def _create_s3_folder(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.s3_bucket)
        folder_name = "raw_data/"
        bucket.put_object(Key=folder_name)
        print(f"S3 folder created: {folder_name}")
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

            # If download was successful, upload to S3 with partitioning
            if file_path:
                self.upload_to_s3_partitioned(
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

    def upload_to_s3(self, file_path: str):
        file_name = os.path.basename(file_path)
        s3_key = f"raw_data/{file_name}"
        self.s3_client.upload_file(file_path, self.s3_bucket, s3_key)
        print(
            f"Uploaded {file_name} to S3 bucket {self.s3_bucket} with key {s3_key}")

    def upload_to_s3_partitioned(self, file_path: str, taxi_type: str, year: int, month: str):
        """
        Upload file to S3 with partitioning structure:
        s3://bucket/taxi_type=yellow/year=2020/month=01/file.parquet
        """
        file_name = os.path.basename(file_path)
        s3_key = f"taxi_type={taxi_type}/year={year}/month={month}/{file_name}"

        try:
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_key)
            print(
                f"Uploaded {file_name} to S3 bucket {self.s3_bucket} with partitioned key: {s3_key}")

            # Optionally delete local file after successful upload
            os.remove(file_path)
            print(f"Removed local file: {file_path}")
        except Exception as e:
            print(f"Error uploading {file_name} to S3: {e}")


async def main():
    downloader = DataDownloadAndLoad(base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/",
                                     taxi_type=["yellow", "green"],
                                     years=list(range(2020, 2023)))  # Years 2020-2025
    await downloader.download_and_load_files()

if __name__ == "__main__":
    asyncio.run(main())
