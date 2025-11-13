import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os
import urllib.request
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description='ingest Parquet Data to Postgres')

# user
# password
# host
# port
# database name 
# table name 
# url of the csv(parquet) 

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    print(f'Downloading {url}...')
    urllib.request.urlretrieve(url, parquet_name)
    print('Download complete.')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    file_parquet = pq.ParquetFile(parquet_name)
    print('Reading schema...')
    df_template = next(file_parquet.iter_batches(batch_size=10)).to_pandas()

    df_template.tpep_pickup_datetime = pd.to_datetime(df_template.tpep_pickup_datetime)
    df_template.tpep_dropoff_datetime = pd.to_datetime(df_template.tpep_dropoff_datetime)

    # --- THÊM CODE NẠP ZONES ---
    print("Uploading zones...")
    # Vì file đã được COPY vào Dockerfile, nó nằm ngay bên cạnh script
    df_zone = pd.read_csv('taxi_zone_lookup.csv')
    df_zone.to_sql(name='zones', con=engine, if_exists='replace')
    print("Zones uploaded!")
    # --------------------------

    df_template.head(0).to_sql(name = table_name, con = engine, if_exists='replace', index = False)
    print(f'Created table {table_name} successfully.')

    df_iter = file_parquet.iter_batches(batch_size=100000)

    t_start = time()
    count = 0

    for batch in df_iter:
        count += 1
        b_start = time()

        batch_df = batch.to_pandas()

        batch_df.tpep_pickup_datetime = pd.to_datetime(batch_df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(batch_df.tpep_dropoff_datetime)

        batch_df.to_sql(name = table_name, con = engine, if_exists='append', index = False)

        b_end = time()
        print(f'Inserted batch {count}, took {b_end - b_start:.3f} second')

    t_end = time()
    print(f'Completed! Total time was {t_end-t_start:.3f} seconds for {count} batches.')


if __name__ == '__main__':
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
