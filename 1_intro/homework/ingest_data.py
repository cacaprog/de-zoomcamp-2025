#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import gzip
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2

    # URLs for the data
    url1 = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
    url2 = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    # Local file names
    csv1 = 'green_taxy.csv.gz'
    csv2 = 'zones.csv'

    # Download the files if they don't already exist
    print('Checking files...')
    if not os.path.exists(csv1):
        print(f'Downloading {csv1}...')
        os.system(f"wget {url1} -O {csv1}")
    else:
        print(f'{csv1} already exists.')

    if not os.path.exists(csv2):
        print(f'Downloading {csv2}...')
        os.system(f"wget {url2} -O {csv2}")
    else:
        print(f'{csv2} already exists.')

    print('Finished downloading files.')

    # Database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Processing Green Taxi Data
    print("Processing green taxi data...")
    try:
        with gzip.open(csv1, 'rt') as f:
            # Create an iterator that will read chunks from the file
            df_iter = pd.read_csv(f, iterator=True, chunksize=100000)

            # Process the first chunk
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            # Write the first chunk to the database
            print(f"Creating table {table_name_1}...")
            df.head(0).to_sql(name=table_name_1, con=engine, if_exists='replace')
            df.to_sql(name=table_name_1, con=engine, if_exists='append')

            # Process the remaining chunks
            for chunk in df_iter:
                t_start = time()
                df = chunk
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                df.to_sql(name=table_name_1, con=engine, if_exists='append')
                t_end = time()
                print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds.')
    except OSError as e:
        print(f"Error reading {csv1}: {e}")
        return

    print('Green Taxi data processing complete!')

    # Processing Zone Data
    print("Processing zones data...")
    try:
        df_zones = pd.read_csv(csv2)
        df_zones.to_sql(name=table_name_2, con=engine, if_exists='replace')
        print('Zones data processing complete!')
    except Exception as e:
        print(f"Error processing zones data: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name_1', required=True, help='name of the green taxi trips table')
    parser.add_argument('--table_name_2', required=True, help='name of the zones table')

    args = parser.parse_args()

    main(args)
