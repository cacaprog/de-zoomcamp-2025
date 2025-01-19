#!/usr/bin/env python
# coding: utf-8
import argparse
import os
import sys
from time import time
import urllib.parse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import requests
from tqdm import tqdm

def download_file(url, output_file):
    """Download a file from URL with progress bar."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    block_size = 8192
    
    with open(output_file, 'wb') as f, tqdm(
        desc=f"Downloading {output_file}",
        total=total_size,
        unit='iB',
        unit_scale=True
    ) as pbar:
        for data in response.iter_content(block_size):
            size = f.write(data)
            pbar.update(size)

def test_connection(engine):
    """Test database connection and permissions."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        return False

def main(params):
    # Unpack parameters
    user = params.user
    password = urllib.parse.quote_plus(params.password)  # Handle special characters in password
    host = params.host
    port = params.port
    db = params.db
    tb = params.tb
    url = params.url
    
    # Validate URL and get file format
    supported_formats = {'.parquet', '.csv'}
    file_suffix = Path(url.lower()).suffix
    
    if file_suffix not in supported_formats:
        print(f'Error: Only {", ".join(supported_formats)} formats are supported')
        sys.exit(1)
    
    # Setup output filename
    file_name = f'output{file_suffix}'
    
    # Download the file
    try:
        download_file(url, file_name)
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        sys.exit(1)

    # Create SQL engine with timeout
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}',
        connect_args={'connect_timeout': 30}
    )
    
    # Test connection
    if not test_connection(engine):
        sys.exit(1)

    try:
        # Read initial chunk for table creation
        if file_suffix == '.parquet':
            file = pq.ParquetFile(file_name)
            df = next(file.iter_batches(batch_size=10)).to_pandas()
            df_iter = file.iter_batches(batch_size=100000)
        else:  # CSV
            df = pd.read_csv(file_name, nrows=10)
            df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

        # Create the table with appropriate data types
        print(f"Creating table {tb}...")
        df.head(0).to_sql(name=tb, con=engine, if_exists='replace', index=False)

        # Insert data with progress tracking
        t_start = time()
        total_rows = 0
        
        with tqdm(desc="Inserting data") as pbar:
            for batch in df_iter:
                if file_suffix == '.parquet':
                    batch_df = batch.to_pandas()
                else:
                    batch_df = batch

                rows = len(batch_df)
                total_rows += rows
                
                batch_df.to_sql(name=tb, con=engine, if_exists='append', index=False)
                pbar.update(rows)

        t_end = time()
        
        # Print summary
        print(f"\nIngestion completed successfully:")
        print(f"Total rows inserted: {total_rows:,}")
        print(f"Total time: {t_end - t_start:.2f} seconds")
        print(f"Average speed: {total_rows / (t_end - t_start):,.2f} rows/second")

    except Exception as e:
        print(f"Error during data ingestion: {str(e)}")
        sys.exit(1)
    finally:
        # Cleanup downloaded file
        try:
            os.remove(file_name)
            print(f"Cleaned up temporary file: {file_name}")
        except OSError:
            pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload data to Postgres')
    parser.add_argument('--user', type=str, help='Postgres user', required=True)
    parser.add_argument('--password', type=str, help='Postgres password', required=True)
    parser.add_argument('--host', type=str, help='Postgres host', required=True)
    parser.add_argument('--port', type=str, help='Postgres port', required=True)
    parser.add_argument('--db', type=str, help='Postgres database', required=True)
    parser.add_argument('--tb', type=str, help='Postgres table', required=True)
    parser.add_argument('--url', type=str, help='URL to download the data', required=True)

    args = parser.parse_args()
    main(args)