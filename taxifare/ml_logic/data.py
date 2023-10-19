import pandas as pd

from google.cloud import bigquery
from colorama import Fore, Style
from pathlib import Path

from taxifare.params import *

def compress(df, **kwargs):
    """
    Reduces the size of the DataFrame by downcasting numerical columns
    """
    df = df.astype(DTYPES_RAW)

    return df

def remove_buggy_transactions(df):

    df = df.drop_duplicates()
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
