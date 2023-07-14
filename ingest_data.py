#!/usr/bin/env python
# coding: utf-8
# from: jupyter nbconvert --to=script [notebook_file_name]
import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    # url = params.url

    csv_name = 'output.csv'

    # os.system(f"wget {url} -O {csv_name}")

    # create postgres connection
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # create a iterable dataframe by reading the csv
    df_iter = pd.read_csv(csv_name,
                          iterator=True, chunksize=100000)

    # move to the next iterable chunk
    df = next(df_iter)

    # change dataframe column types
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create table in postgres
    df.head(n=0).to_sql(name=table_name,
                        con=engine, if_exists='replace')

    # insert first dataframe chunk into postgres
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # insert remaining data to postgres
    while True:
        t_start = time()
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='postgres user name')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host adress')
    parser.add_argument('--port', help='postgres port number')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--table_name', help='table name')
    # parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
