import os
import pandas as pd
from sqlalchemy import create_engine
import argparse
import requests

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table = args.table
    url = args.url

    # ! wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

    filename = url.split("/")[-1]
    
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
        df = pd.read_csv(f'{url}', compression='gzip', low_memory=False)
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # create a table
    df.head(0).to_sql(name=table, con=engine, if_exists='replace')
    
    # insert data
    df_iter = pd.read_csv(filename, compression='gzip', iterator=True, chunksize=100000, low_memory=False)

    for chunk_df in df_iter:
        print(len(chunk_df))
        chunk_df.lpep_pickup_datetime = pd.to_datetime(chunk_df.lpep_pickup_datetime)
        chunk_df.lpep_dropoff_datetime = pd.to_datetime(chunk_df.lpep_dropoff_datetime)
        chunk_df.to_sql(name=table, con=engine, if_exists='append')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user')
    parser.add_argument('--password', help='password')
    parser.add_argument('--host', help='host')
    parser.add_argument('--port', help='port')
    parser.add_argument('--db', help='database')
    parser.add_argument('--table', help='table')
    parser.add_argument('--url', help='URL')
    args = parser.parse_args()
    main(args)
