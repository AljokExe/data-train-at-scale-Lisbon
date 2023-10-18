import pandas as pd

from google.cloud import bigquery
from colorama import Fore, Style
from pathlib import Path

from taxifare.params import *

def compress(df, **kwargs):
    """
    Reduces the size of the DataFrame by downcasting numerical columns
    """
    input_size = df.memory_usage(index=True).sum()/ 1024**2
    print("old dataframe size: ", round(input_size,2), 'MB')

    in_size = df.memory_usage(index=True).sum()

    for t in ["float", "integer"]:
        l_cols = list(df.select_dtypes(include=t))

        for col in l_cols:
            df[col] = pd.to_numeric(df[col], downcast=t)

    out_size = df.memory_usage(index=True).sum()
    ratio = (1 - round(out_size / in_size, 2)) * 100

    print("optimized size by {} %".format(round(ratio,2)))
    print("new DataFrame size: ", round(out_size / 1024**2,2), " MB")

    return df

def remove_buggy_transactions(df):

    df = df.dropna(how='any', axis=0)
    df = df[(df.dropoff_latitude != 0) | (df.dropoff_longitude != 0) | (df.pickup_latitude != 0) | (df.pickup_longitude != 0)]
    df = df[df.passenger_count > 0]
    df = df[df.fare_amount > 0]
    # Let's cap training set to reasonable values
    df = df[df.fare_amount < 400]
    df = df[df.passenger_count < 8]

    return df

def geo_irrel_transac(df):
    df = df[df["pickup_latitude"].between(left=40.5, right=40.9)]
    df = df[df["dropoff_latitude"].between(left=40.5, right=40.9)]
    df = df[df["pickup_longitude"].between(left=-74.3, right=-73.7)]
    df = df[df["dropoff_longitude"].between(left=-74.3, right=-73.7)]

    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw data by
    - assigning correct dtypes to each column
    - removing buggy or irrelevant transactions
    """
    # Compress raw_data by setting types to DTYPES_RAW
    df_compressed=compress(df)

    # Remove buggy transactions
    df_removed_buggy=remove_buggy_transactions(df_compressed)

    # Remove geographically irrelevant transactions (rows)
    df_cleaned=geo_irrel_transac(df_removed_buggy)
    print("✅ data cleaned")

    return df_cleaned
