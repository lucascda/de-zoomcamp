#!/usr/bin/env python
# coding: utf-8
# from: jupyter nbconvert --to=script [notebook_file_name]
# python ingest_data.py --user=root --password=root --host=localhost --port=5430 --db=ny_taxi --table_name=yellow_taxi_trips --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash


@task(log_prints=True)
def transform_data(df):
    print(
        f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(
        f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # create a iterable dataframe by reading the csv
    df_iter = pd.read_csv(csv_name,
                          iterator=True, chunksize=100000)

    # move to the next iterable chunk
    df = next(df_iter)

    # change dataframe column types
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True, retries=3)
def ingest_data(params, df):

    # params from args
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    # create postgres connection
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # create table in postgres
    df.head(n=0).to_sql(name=table_name,
                        con=engine, if_exists='replace')

    # insert first dataframe chunk into postgres
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # insert remaining data to postgres
    # while True:
    #     try:

    #         t_start = time()
    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk..., took %.3f second' %
    #               (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Flow")
def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='postgres user name')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host adress')
    parser.add_argument('--port', help='postgres port number')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    log_subflow(args.table_name)
    raw_data = extract_data(args.url)
    data = transform_data(raw_data)
    ingest_data(args, data)


if __name__ == '__main__':
    main()
