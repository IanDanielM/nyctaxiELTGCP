import asyncio
import os
from datetime import datetime
from typing import List, Optional, Set, Tuple

import httpx
from google.cloud import dataflow_v1beta3, storage


def handler(request):
    """
    Cloud Function entry point. Triggered by HTTP request.
    """
    request_json = request.get_json(silent=True)
    
    print("Function triggered by HTTP request.")
    if request_json:
        print(f"Received JSON payload: {request_json}")
    
    years = request_json.get('years') if request_json else None
    taxi_types = request_json.get('taxi_types') if request_json else None
    months = request_json.get('months') if request_json else None
    force_reprocess = request_json.get('force_reprocess', False) if request_json else False

    asyncio.run(run_data(years, taxi_types, months, force_reprocess))
    
    print("Function execution finished successfully.")
    return ("OK", 200)

async def run_data(years: Optional[List[int]], taxi_types: Optional[List[str]], months: Optional[List[int]], force_reprocess: bool):
    """Initializes, runs the download, and then launches batch Dataflow job"""
    
    if not years or not taxi_types:
        current_year = datetime.now().year
        default_years = [current_year - 1, current_year]
        default_taxi_types = ["yellow", "green"]
        years = years or default_years
        taxi_types = taxi_types or default_taxi_types
    
    print(f"Checking data for years: {years}, taxi types: {taxi_types}")
    
    downloader = DataDownloadAndLoad(
        base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/",
        taxi_type=taxi_types,
        years=years,
        months=months,
        force_reprocess=force_reprocess
    )
    
    new_data_combos = await downloader.download_and_load_files()
    
    if not new_data_combos:
        print("No new files were downloaded. Skipping Dataflow job launch.")
        return

    print(f"New data found for: {new_data_combos}. Launching Dataflow job...")
    for year, taxi_type in new_data_combos:
        launch_batch_dataflow_job(taxi_type=taxi_type, year=year)
            
    print("All required batch Dataflow jobs have been launched.")


def launch_batch_dataflow_job(taxi_type: str, year: int):
    PROJECT_ID = os.environ.get('PROJECT_ID', "dataengineer-463405")
    GCS_TEMPLATE_PATH = os.environ.get('GCS_TEMPLATE_PATH', "gs://dataflow-101253/templates/taxi-data-processor.json")
    BQ_OUTPUT_TABLE = os.environ.get('BQ_OUTPUT_TABLE', "dataengineer-463405:taxi_data.standardized_trips")
    LOCATION = "europe-west1"
    BUCKET_NAME = 'nyc-taxi-data-101253'

    input_path_pattern = f"gs://{BUCKET_NAME}/taxi_type={taxi_type}/year={year}/month=*/*.parquet"
    print(f"Launching Dataflow job for input pattern: {input_path_pattern}")
    print(f"Output table: {BQ_OUTPUT_TABLE}")
    print(f"Using input pattern: {input_path_pattern} for taxi type: {taxi_type}, year: {year}")
    parameters = {"input_file_pattern": input_path_pattern, "output_table": BQ_OUTPUT_TABLE}
    
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    job_name = f"batch-process-{taxi_type}-{year}-{timestamp}"

    try:
        dataflow_client = dataflow_v1beta3.FlexTemplatesServiceClient()
        request = dataflow_v1beta3.LaunchFlexTemplateRequest(
            project_id=PROJECT_ID,
            location=LOCATION,
            launch_parameter=dataflow_v1beta3.LaunchFlexTemplateParameter(
                job_name=job_name,
                container_spec_gcs_path=GCS_TEMPLATE_PATH,
                parameters=parameters,
            )
        )
        response = dataflow_client.launch_flex_template(request=request)
        print(f"Successfully launched batch Dataflow job: {response.job.id} for {taxi_type} {year}")
    except Exception as e:
        print(f"Error launching batch Dataflow job for {taxi_type} {year}: {str(e)}")
        raise e


class DataDownloadAndLoad:
    def __init__(self, base_url: str, taxi_type: list, years: list, months: Optional[list] = None, force_reprocess: bool = False):
        self.base_url = base_url
        self.taxi_type = taxi_type
        self.years = years
        self.months = months
        self.force_reprocess = force_reprocess
        self.download_dir = "/tmp/data/raw"
        self.cloud_storage_client = storage.Client()
        self.bucket_name = 'nyc-taxi-data-101253'
        os.makedirs(self.download_dir, exist_ok=True)

    async def download_file(self, url: str, session: httpx.AsyncClient):
        try:
            response = await session.get(url, timeout=120)
            response.raise_for_status()
            file_name = url.split("/")[-1]
            file_path = os.path.join(self.download_dir, file_name)
            with open(file_path, "wb") as f: f.write(response.content)
            print(f"Successfully downloaded {file_name}")
            return file_path
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404: print(f"File not found at {url} (404), skipping.")
            else: print(f"Failed to download {url}. Status code: {e.response.status_code}")
            return None
        except Exception as e:
            print(f"An error occurred while downloading {url}: {e}")
            return None

    def file_exists_in_gcs(self, taxi_type: str, year: int, month: str) -> bool:
        bucket = self.cloud_storage_client.bucket(self.bucket_name)
        prefix = f"taxi_type={taxi_type}/year={year}/month={month}/"
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
        if blobs:
            print(f"File already exists for {taxi_type} {year}-{month}, skipping download")
            return True
        return False

    async def process_file(self, year: int, month: int, taxi: str, session: httpx.AsyncClient) -> Optional[Tuple[int, str]]:
        """Downloads and uploads a file. Returns (year, taxi_type) on success, else None."""
        month_str = str(month).zfill(2)
        print(f"Processing {taxi} data for {year}-{month_str}")
        
        if not self.force_reprocess and self.file_exists_in_gcs(taxi, year, month_str):
            return None # SKIPPED
            
        url = f"{self.base_url}{taxi}_tripdata_{year}-{month_str}.parquet"
        file_path = await self.download_file(url, session)
        if file_path:
            self.upload_to_bucket_partitioned(file_path, taxi, year, month_str)
            return (year, taxi) # SUCCESS, new file uploaded
        return None

    async def download_and_load_files(self) -> Set[Tuple[int, str]]:
        """Runs all download/upload tasks and returns a set of (year, taxi_type) with new data."""
        tasks = []
        current_year = datetime.now().year
        current_month = datetime.now().month

        async with httpx.AsyncClient() as client:
            for year in self.years:
                for month in (self.months or range(1, 13)):
                    if year == current_year and month > current_month: continue
                    for taxi in self.taxi_type:
                        tasks.append(self.process_file(year, month, taxi, client))
            
            if not tasks:
                print("No files to process based on current parameters.")
                return set()

            results = await asyncio.gather(*tasks)
            new_data_combos = {res for res in results if res is not None}
            return new_data_combos

    def upload_to_bucket_partitioned(self, file_path: str, taxi_type: str, year: int, month: str):
        file_name = os.path.basename(file_path)
        bucket_key = f"taxi_type={taxi_type}/year={year}/month={month}/{file_name}"
        try:
            bucket = self.cloud_storage_client.bucket(self.bucket_name)
            blob = bucket.blob(bucket_key)
            blob.upload_from_filename(file_path)
            print(f"Uploaded {file_name} to gs://{self.bucket_name}/{bucket_key}")
            os.remove(file_path)
        except Exception as e:
            print(f"Error uploading {file_name} to bucket: {e}")
