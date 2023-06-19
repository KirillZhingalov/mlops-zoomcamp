#!/usr/bin/env python
# coding: utf-8


import pickle
import pandas as pd
import numpy as np
import typing as tp
import argparse as ap


def parse_args():
    parser = ap.ArgumentParser()
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--month', type=int, required=True)
    return parser.parse_args()


def read_data(filename, cat_features: tp.List[str]):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[cat_features] = df[cat_features].fillna(-1).astype('int').astype('str')
    
    return df


def main(year: int, month: int):
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)

    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(
        f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet', 
        cat_features=categorical
    )

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df['duration'] = y_pred

    df_result = df[['ride_id', 'duration']].copy()
    output_file = './results.parquet'

    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )

    print('Mean predicted duration:', df_result['duration'].mean())


if __name__ == '__main__':
    args = parse_args()
    main(args.year, args.month)