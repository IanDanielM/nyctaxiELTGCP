import argparse
import logging
import re
from typing import Any, Dict, Generator

import apache_beam as beam
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


class TaxiDataStandardizer(beam.DoFn):
    """Standardize taxi data schema and prepare for BigQuery using PyArrow."""
    
    def __init__(self):
        self.column_mapping = self._get_column_mapping()
        self.processed_count = 0
    
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
        }
    
    @staticmethod
    def get_bigquery_schema() -> str:
        """Define BigQuery table schema."""
        return (
            "vendor_id:INTEGER, pickup_datetime:TIMESTAMP, dropoff_datetime:TIMESTAMP, "
            "passenger_count:INTEGER, trip_distance:FLOAT, rate_code_id:INTEGER, "
            "payment_type:INTEGER, fare_amount:FLOAT, extra:FLOAT, "
            "tip_amount:FLOAT, total_amount:FLOAT, pickup_location_id:INTEGER, "
            "dropoff_location_id:INTEGER, trip_type:INTEGER, taxi_type:STRING, "
            "year:INTEGER, month:INTEGER"
        )
    
    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Process each row to standardize schema."""
        try:          
            # Apply column name mapping
            mapped_element = {self.column_mapping.get(k, k): v for k, v in element.items()}
            
            standardized_row = {
                'vendor_id': self._safe_convert(mapped_element.get('vendor_id'), int),
                'pickup_datetime': self._convert_timestamp(mapped_element.get('pickup_datetime')),
                'dropoff_datetime': self._convert_timestamp(mapped_element.get('dropoff_datetime')),
                'passenger_count': self._safe_convert(mapped_element.get('passenger_count'), int),
                'trip_distance': self._safe_convert(mapped_element.get('trip_distance'), float),
                'rate_code_id': self._safe_convert(mapped_element.get('rate_code_id'), int),
                'payment_type': self._safe_convert(mapped_element.get('payment_type'), int),
                'fare_amount': self._safe_convert(mapped_element.get('fare_amount'), float),
                'extra': self._safe_convert(mapped_element.get('extra'), float),
                'tip_amount': self._safe_convert(mapped_element.get('tip_amount'), float),
                'total_amount': self._safe_convert(mapped_element.get('total_amount'), float),
                'pickup_location_id': self._safe_convert(mapped_element.get('pickup_location_id'), int),
                'dropoff_location_id': self._safe_convert(mapped_element.get('dropoff_location_id'), int),
                'trip_type': self._safe_convert(mapped_element.get('trip_type'), int),
                'taxi_type': mapped_element.get('taxi_type'),
                'year': mapped_element.get('year'),
                'month': mapped_element.get('month')
            }
            
            yield standardized_row
            
        except Exception as e:
            logging.error(f"Error processing row: {element}. Error: {e}")

    def _safe_convert(self, value, target_type):
        """convert values to target type"""
        if value is None or value == '':
            return None
        try:
            if target_type == int:
                return int(float(value))
            elif target_type == float:
                return float(value)
            return target_type(value)
        except (ValueError, TypeError):
            return None

    def _convert_timestamp(self, value):
        """Convert timestamp values to string format for BigQuery."""
        if value is None:
            return None
        
        if hasattr(value, 'to_pydatetime'):
            return value.to_pydatetime().isoformat()
        
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        
        if isinstance(value, str):
            return value
            
        return str(value)


class ReadParquetAndAddMetadata(beam.DoFn):
    """
    Read Parquet files using PyArrow
    """
    
    def __init__(self, batch_size=50000):  # Reduced batch size for better memory management
        self.batch_size = batch_size
        self.processed_files = 0
    
    def process(self, file: beam.io.fileio.ReadableFile):
        file_path = file.metadata.path
        self.processed_files += 1
        logging.info(f"Processing file {self.processed_files}: {file_path}")

        # More flexible regex to handle different path structures
        match = re.search(r"taxi_type=(\w+)/year=(\d{4})/month=(\d{1,2})", file_path)
        if not match:
            # Try alternative pattern
            match = re.search(r"(\w+)_tripdata_(\d{4})-(\d{2})", file_path)
            if match:
                file_metadata = {
                    'taxi_type': match.group(1),
                    'year': int(match.group(2)),
                    'month': int(match.group(3))
                }
            else:
                logging.warning(f"Could not parse metadata from path: {file_path}")
                return
        else:
            file_metadata = {
                'taxi_type': match.group(1),
                'year': int(match.group(2)),
                'month': int(match.group(3))
            }

        try:
            with file.open() as f:
                # Read entire file into memory for smaller files or use batch reading
                try:
                    table = pq.read_table(f)
                    total_rows = len(table)
                    
                    logging.info(f"File has {total_rows} rows")
                    
                    # Add metadata columns
                    taxi_type_array = pa.array([file_metadata['taxi_type']] * total_rows)
                    year_array = pa.array([file_metadata['year']] * total_rows)
                    month_array = pa.array([file_metadata['month']] * total_rows)
                    
                    table = table.append_column('taxi_type', taxi_type_array)
                    table = table.append_column('year', year_array)
                    table = table.append_column('month', month_array)
                    
                    # Convert to list of dictionaries
                    records = table.to_pylist()
                    
                    for record in records:
                        yield record
                    
                    logging.info(f"Completed processing {total_rows} records from {file_path}")
                    
                except Exception as batch_error:
                    # Fallback to batch reading if memory issues
                    logging.warning(f"Fallback to batch reading for {file_path}: {batch_error}")
                    f.seek(0)  # Reset file pointer
                    parquet_file = pq.ParquetFile(f)
                    
                    for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                        batch_size = len(batch)
                        taxi_type_array = pa.array([file_metadata['taxi_type']] * batch_size)
                        year_array = pa.array([file_metadata['year']] * batch_size)
                        month_array = pa.array([file_metadata['month']] * batch_size)
                        
                        enhanced_batch = batch.append_column('taxi_type', taxi_type_array)
                        enhanced_batch = enhanced_batch.append_column('year', year_array)
                        enhanced_batch = enhanced_batch.append_column('month', month_array)
                        
                        batch_records = enhanced_batch.to_pylist()
                        
                        for record in batch_records:
                            yield record
                
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            # Don't re-raise the exception to avoid stopping the entire pipeline
            return


def run_dataflow_pipeline(argv=None):
    """
    Optimized Dataflow pipeline using PyArrow native operations.
    
    Args:
        input_pattern (str): GCS file pattern to read from.
        output_table (str): BigQuery table to write to.
        batch_size (int): Batch size for reading parquet files.
        pipeline_options (PipelineOptions): The pipeline options.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file_pattern',
        required=True,
        help='GCS file pattern to read data from (e.g., gs://bucket/path/*/*.parquet)'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery table to write results to (e.g., project:dataset.table).'
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=50000,
        help='Batch size for reading parquet files (default: 50000)'
    )
    parser.add_argument(
        '--max_workers',
        type=int,
        default=8,
        help='Maximum number of workers'
    )

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(
        pipeline_args + [
            f'--max_num_workers={known_args.max_workers}',
            '--disk_size_gb=100',
            '--worker_machine_type=n1-standard-4'
        ]
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Find Files' >> MatchFiles(known_args.input_file_pattern)
            | 'Read Files' >> ReadMatches()
            | 'Read and Add Metadata' >> beam.ParDo(
                ReadParquetAndAddMetadata(batch_size=known_args.batch_size)
            )
            | 'Standardize Schema' >> beam.ParDo(TaxiDataStandardizer())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=TaxiDataStandardizer.get_bigquery_schema(),
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND',
                additional_bq_parameters={
                    'clustering': {'fields': ['taxi_type', 'year', 'month']},
                    'timePartitioning': {'type': 'DAY', 'field': 'pickup_datetime'}
                }
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_dataflow_pipeline()