from loguru import logger
import datetime
import pandas as pd
from configparser import ConfigParser
import boto3
from io import StringIO

config = ConfigParser()
config.read('config.ini')

access_key = config['aws']['access_key']
secret_access_key = config['aws']['secret_access_key']
bucket_name = config['aws']['bucket_name']          # ADD THIS in config.ini
incoming_prefix = config['aws']['incoming_prefix']  # example: incoming_files/

logger.info(f'access_key: {access_key}')
logger.info(f'secret_access_key: {secret_access_key}')

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key
)


def list_csv_from_s3():
    date_file = datetime.datetime.today().strftime('%Y%m%d')
    prefix = f"{incoming_prefix}{date_file}/"  # folder inside bucket
    logger.info(f"Checking S3 prefix: {prefix}")

    csv_keys = []
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            logger.error("No files found in S3")
            return []

        for item in response["Contents"]:
            key = item["Key"]
            if key.lower().endswith(".csv"):
                csv_keys.append(key)

        return csv_keys

    except Exception as e:
        logger.error(f"Error listing S3 CSV files: {e}")
        return []


def read_incoming_file():
    csv_keys = list_csv_from_s3()
    dfs = []

    for key in csv_keys:
        try:
            logger.info(f"Reading from S3: {key}")

            obj = s3.get_object(Bucket=bucket_name, Key=key)
            csv_data = obj["Body"].read().decode("utf-8")

            df = pd.read_csv(
                StringIO(csv_data),
                encoding="utf-8",
                on_bad_lines="skip",
                parse_dates=["order_date"],
                date_format="mixed",
                dayfirst=True
            )

            dfs.append(df)

        except Exception as e:
            logger.error(f"Failed to read S3 file {key}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def find_errors(df):
    df['reason'] = ""

    df.loc[~df['city'].isin(['Bangalore', 'Mumbai']), 'reason'] += "City is not correct; "
    df.loc[df['order_date'].isna(), 'reason'] += "Date is not correct; "
    df.loc[df['price'].isna(), 'reason'] += "Price is not correct; "
    df.loc[df['quantity'].isna(), 'reason'] += "Quantity is not correct; "
    df.loc[df['product_id'].isna(), 'reason'] += "Product ID is not correct; "
    df.loc[df['sale_actual_price'] > df['sales'], 'reason'] += "Price is less than sales; "

    df['reason'] = df['reason'].str.rstrip("; ")
    return df


def filter_data_save(df):
    df_error = df[df['reason'] != ""][['order_id', 'order_date', 'product_id',
                                       'quantity', 'sales', 'city', 'reason']] \
        .sort_values(['order_id', 'order_date']).reset_index(drop=True)

    df_clean = df[df['reason'] == ""][['order_id', 'order_date', 'product_id',
                                       'quantity', 'sales', 'city']] \
        .sort_values(['order_id', 'order_date']).reset_index(drop=True)

    return df_clean, df_error


if __name__ == '__main__':
    logger.info(read_incoming_file())
