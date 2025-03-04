import asyncio
import os
from datetime import datetime

import httpx


class DataDownloadAndLoad:
    def __init__(self, base_url: str, taxi_type: list, years: list):
        self.base_url = base_url
        self.taxi_type = taxi_type
        self.years = years
        self.download_dir = "data/raw"
        os.makedirs(self.download_dir, exist_ok=True)

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
            print(taxi_type)
            url = f"{self.base_url}{taxi_type}_tripdata_{year}-{month_str}.parquet"
            print(url)
            file_path = await self.download_file(url)

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


async def main():
    downloader = DataDownloadAndLoad(base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/",
                                     taxi_type=["yellow"], years=[2016])
    await downloader.download_and_load_files()

if __name__ == "__main__":
    asyncio.run(main())
