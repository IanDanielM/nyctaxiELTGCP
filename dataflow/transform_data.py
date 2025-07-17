import argparse
import logging
from typing import Any, Dict, Generator

import apache_beam as beam
import pandas as pd
from apache_beam.io import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


class TaxiDataStandardizer(beam.DoFn):
    """Standardize taxi data schema and prepare for BigQuery."""
    
    def __init__(self):
        self.column_mapping = self._get_column_mapping()
    
    def _get_column_mapping(self) -> Dict[str, str]:
        """Map various column names to standardized names."""
        return {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'lpep_pickup_datetime': 'pickup_datetime',
            'lpep_dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'RatecodeID': 'rate_code_id',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            'trip_distance': 'trip_distance',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'tip_amount': 'tip_amount',
            'total_amount': 'total_amount',
            'payment_type': 'payment_type',
            'trip_type': 'trip_type',
            'taxi_type': 'taxi_type'
        }
    
    @staticmethod
    def get_bigquery_schema() -> str:
        """Define BigQuery table schema."""
        return (
            "vendor_id:INTEGER, pickup_datetime:TIMESTAMP, dropoff_datetime:TIMESTAMP, "
            "passenger_count:INTEGER, trip_distance:FLOAT, rate_code_id:INTEGER, "
            "payment_type:INTEGER, fare_amount:FLOAT, extra:FLOAT, mta_tax:FLOAT, "
            "tip_amount:FLOAT, total_amount:FLOAT, pickup_location_id:INTEGER, "
            "dropoff_location_id:INTEGER, trip_type:INTEGER, taxi_type:STRING, "
            "year:INTEGER, month:INTEGER"
        )
    
    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Process each row to standardize schema."""
        try:
            # Apply column name mapping
            mapped_element = {}
            for key, value in element.items():
                mapped_key = self.column_mapping.get(key, key)
                mapped_element[mapped_key] = value
            
            # Create standardized row
            standardized_row = {
                'vendor_id': self._safe_int(mapped_element.get('vendor_id')),
                'pickup_datetime': self._safe_timestamp(mapped_element.get('pickup_datetime')),
                'dropoff_datetime': self._safe_timestamp(mapped_element.get('dropoff_datetime')),
                'passenger_count': self._safe_int(mapped_element.get('passenger_count')),
                'trip_distance': self._safe_float(mapped_element.get('trip_distance')),
                'rate_code_id': self._safe_int(mapped_element.get('rate_code_id')),
                'payment_type': self._safe_int(mapped_element.get('payment_type')),
                'fare_amount': self._safe_float(mapped_element.get('fare_amount')),
                'extra': self._safe_float(mapped_element.get('extra')),
                'tip_amount': self._safe_float(mapped_element.get('tip_amount')),
                'total_amount': self._safe_float(mapped_element.get('total_amount')),
                'pickup_location_id': self._safe_int(mapped_element.get('pickup_location_id')),
                'dropoff_location_id': self._safe_int(mapped_element.get('dropoff_location_id')),
                'trip_type': self._safe_int(mapped_element.get('trip_type')),
                
                # These already use the correct keys from the source element
                'taxi_type': mapped_element.get('_taxi_type'),
                'year': mapped_element.get('_year'),
                'month': mapped_element.get('_month')
            }
            
            yield standardized_row
            
        except Exception as e:
            logging.error(f"Error processing row: {e}")
            # Log the error but continue processing
    
    def _safe_int(self, value):
        """Safely convert to integer."""
        if value is None or pd.isna(value):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert to float."""
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_string(self, value):
        """Safely convert to string."""
        if value is None or pd.isna(value):
            return None
        return str(value)
    
    def _safe_timestamp(self, value):
        """Safely convert to timestamp."""
        if value is None or pd.isna(value):
            return None
        try:
            return pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None


class ExtractDateMetadata(beam.DoFn):
    """Extract year and month from pickup_datetime."""
    
    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Extract date metadata from pickup datetime."""
        try:
            # Get pickup datetime - try different column names
            pickup_dt = (element.get('tpep_pickup_datetime') or 
                        element.get('lpep_pickup_datetime') or 
                        element.get('pickup_datetime'))
            
            if pickup_dt:
                if isinstance(pickup_dt, str):
                    dt = pd.to_datetime(pickup_dt)
                else:
                    dt = pickup_dt
                
                element['_year'] = dt.year
                element['_month'] = dt.month
            else:
                element['_year'] = None
                element['_month'] = None
            
            yield element
            
        except Exception as e:
            logging.error(f"Error extracting date metadata: {e}")
            element['_year'] = None
            element['_month'] = None
            yield element


def run_dataflow_pipeline(argv=None):
    """Run Dataflow pipeline to process raw data and load to BigQuery."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_bucket', required=True, help='GCS bucket containing raw data')
    parser.add_argument('--output_table', required=True, help='BigQuery table to write results to')
    parser.add_argument('--year', type=int, default=None, help='Year of the data to process(Optional)')
    parser.add_argument('--month', type=int, default=None, help='Month of the data to process (optional)')
    known_args, pipeline_args = parser.parse_known_args(argv)


    # Pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        taxi_types = ['yellow', 'green']
        all_data = []
        month_glob = str(known_args.month).zfill(2) if known_args.month else '*'
        year_glob = str(known_args.year) if known_args.year else '*'
        
        for taxi_type in taxi_types:
            # Read data for specific taxi type
            input_path = (
                f'gs://{known_args.input_bucket}/taxi_type={taxi_type}/'
                f'year={year_glob}/month={month_glob}/*.parquet'
            )
            taxi_data = (
                pipeline
                | f'Read {taxi_type.title()} from {known_args.year}/{month_glob}' >> ReadFromParquet(input_path)
                | f'Add {taxi_type.title()} Metadata' >> beam.Map(lambda record, t_type=taxi_type: {**record, '_taxi_type': t_type})
                | f'Extract Date Metadata {taxi_type.title()}' >> beam.ParDo(ExtractDateMetadata())
                | f'Standardize {taxi_type.title()} Schema' >> beam.ParDo(TaxiDataStandardizer())
            )
            all_data.append(taxi_data)
        
        # Combine all taxi types
        combined_data = tuple(all_data) | 'Combine All Taxi Types' >> beam.Flatten()
        
        # Write to BigQuery
        (
            combined_data
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=TaxiDataStandardizer.get_bigquery_schema(),
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                additional_bq_parameters={
                    'clustering': {'fields': ['taxi_type', 'year', 'month']},
                    'timePartitioning': {'type': 'DAY', 'field': 'pickup_datetime'}
                }
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_dataflow_pipeline()