# NYC Taxi Data Pipeline - ELT Project

An end-to-end ELT (Extract, Load, Transform) pipeline for processing NYC taxi trip data using Google Cloud Platform services and modern data engineering tools.

## Architecture Overview

This project implements a scalable, cloud-native data pipeline that:

1. **Extracts** NYC taxi trip data from the official NYC Open Data API
2. **Loads** raw data into Google Cloud Storage and BigQuery
3. **Transforms** data using dbt to create analytics-ready dimensional models
4. **Orchestrates** the entire pipeline using Google Cloud services

### Architecture Diagram

## Tech Stack

- **Cloud Platform**: Google Cloud Platform (GCP)
- **Data Processing**: Apache Beam / Google Dataflow
- **Data Warehouse**: Google BigQuery
- **Transformation**: dbt (data build tool)
- **Containerization**: Docker
- **CI/CD**: Google Cloud Build
- **Orchestration**: Google Cloud Functions, Cloud Run
- **Languages**: Python, SQL


## Components

### 1. Data Ingestion (Cloud Function)
- **Location**: `scripts/data-import-fn/`
- **Purpose**: downloads NYC taxi data from the official API
- **Features**:
  - Configurable date ranges and taxi types
  - Incremental data loading
  - Error handling and retry logic
  - Uploads to Google Cloud Storage

### 2. Data Processing (Dataflow)
- **Location**: `dataflow/`
- **Purpose**: Processes raw parquet files and standardizes the schema
- **Features**:
  - Schema standardization across different taxi types
  - Data validation and cleaning
  - Direct loading to BigQuery

### 3. Data Transformation (dbt)
- **Location**: `cloudrundbt/dbt/`
- **Purpose**: Creates analytics-ready dimensional models
- **Models**:
  - **Silver Layer**: Cleaned and standardized data (`sl_trip_data.sql`)
  - **Gold Layer**: Dimensional models and fact tables
    - `fct_trip.sql` - Trip fact table
    - `dim_date.sql` - Date dimension
    - `dim_locations.sql` - Location dimension
    - `dim_payments.sql` - Payment type dimension
    - `dim_rate_types.sql` - Rate code dimension
    - `dim_taxi_types.sql` - Taxi type dimension
    - `dim_time.sql` - Time dimension
    - `trip_metrics.sql` - Aggregated metrics


## Getting Started

### Prerequisites

1. Google Cloud Platform account with billing enabled
3. Docker installed
4. Python 3.10+
5. uv package manager

### Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd nyc_taxi_project
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Set up GCP credentials**:
   ```bash
   # Place your service account key in gcloudkeys.json
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcloudkeys.json
   ```

4. **Build and deploy containers**:
   ```bash
   # Deploy Dataflow template
   cd dataflow
   gcloud builds submit --config cloudbuild.yaml
   
   # Deploy dbt container
   cd ../cloudrundbt
   gcloud builds submit --config cloudbuild.yaml
   ```

### Running the Pipeline

1. **Trigger data ingestion**:
   ```bash
   # Call the Cloud Function with desired parameters
   curl -X POST <cloud-function-url> \
     -H "Content-Type: application/json" \
     -d '{
       "years": [2023, 2024],
       "taxi_types": ["yellow", "green"],
       "months": [1, 2, 3]
     }'
   ```

2. **Run data processing**:
   ```bash
   gcloud dataflow flex-template run "taxi-data-job-$(date +%Y%m%d-%H%M%S)" \
     --template-file-gcs-location "gs://your-bucket/templates/taxi-data-processor.json" \
     --region "your-region" \
     --parameters input_bucket="your-input-bucket" \
     --parameters output_table="your-project:dataset.table"
   ```

3. **Execute dbt transformations**:
   ```bash
   cd cloudrundbt/dbt
   dbt run
   dbt test
   ```

## Data Models

### Source Data
- **Table**: `nyctaxi.standardized_trips`
- **Schema**: Standardized across yellow and green
- **Partitioning**: By pickup date
- **Clustering**: By location and payment type

### Gold Layer Tables

| Table | Description | Key Features |
|-------|-------------|--------------|
| `fct_trip` | Trip fact table | Partitioned by date, clustered by location/payment |
| `dim_date` | Date dimension | Full calendar with business day flags |
| `dim_locations` | NYC taxi zones | Borough, zone names, service zones |
| `dim_payments` | Payment methods | Cash, credit, dispute, etc. |
| `dim_rate_types` | Rate codes | Standard, airport, negotiated, etc. |
| `dim_taxi_types` | Vehicle types | Yellow, green, FHV |
| `dim_time` | Time dimension | Hour, minute, period breakdowns |
| `trip_metrics` | Aggregated metrics | Daily/hourly trip statistics |

## 🔍 Data Quality

- **dbt tests**: Built-in data quality checks
- **Schema validation**: Enforced during Dataflow processing
- **Anomaly detection**: Outlier identification in transformations
- **Referential integrity**: Foreign key relationships maintained

## Deployment

### Cloud Build Pipelines

1. **Dataflow Pipeline**:
   ```bash
   cd dataflow
   gcloud builds submit --config cloudbuild.yaml
   ```

2. **dbt Pipeline**:
   ```bash
   cd cloudrundbt
   gcloud builds submit --config cloudbuild.yaml
   ```