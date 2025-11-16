import pandas as pd
import numpy as np
import datetime
from loguru import logger
from configparser import ConfigParser
import boto3
from io import StringIO
from incoming_files import read_incoming_file   # ← already modified for S3
from botocore.exceptions import ClientError

# -------------------------------
# STEP 1: SETUP AWS
# -------------------------------
config = ConfigParser()
config.read('config.ini')

access_key = config['aws']['access_key']
secret_access_key = config['aws']['secret_access_key']
bucket_name = config['aws']['bucket_name']
success_prefix = config['aws']['success_prefix']
rejected_prefix = config['aws']['rejected_prefix']

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key
)

# -------------------------------
# STEP 2: READ INPUT FILE
# -------------------------------
df = read_incoming_file()
master_product_df = pd.read_csv('product_master.csv')

df = df.merge(master_product_df, how='left', on='product_id')
df['sale_actual_price'] = df['quantity'] * df['price']

df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d', errors='coerce')

# -------------------------------
# STEP 3: FIND ERRORS
# -------------------------------
def find_errors(df):
    df['reason'] = ""

    df.loc[~df['city'].isin(['Bangalore', 'Mumbai']), 'reason'] += "City is not correct; "
    df.loc[df['order_date'].isna(), 'reason'] += "Date is not correct; "
    df.loc[df['price'].isna(), 'reason'] += "Price is not correct; "
    df.loc[df['quantity'].isna(), 'reason'] += "Quantity is not correct; "
    df.loc[df['product_id'].isna(), 'reason'] += "Product ID is not correct; "
    df.loc[df['sale_actual_price'] > df['sales'], 'reason'] += "Price is less than Actual price; "

    df['reason'] = df['reason'].str.rstrip("; ")

    return df


df = find_errors(df)

# -------------------------------
# STEP 4: SPLIT – CLEAN & ERROR
# -------------------------------
df_error = df[df['reason'] != ""][[
    'order_id', 'order_date', 'product_id', 'quantity', 'sales', 'city', 'reason'
]].sort_values(['order_id', 'order_date']).reset_index(drop=True)

df_clean = df[df['reason'] == ""][[
    'order_id', 'order_date', 'product_id', 'quantity', 'sales', 'city'
]].sort_values(['order_id', 'order_date']).reset_index(drop=True)


# -------------------------------
# STEP 5: CREATE S3 DATE FOLDER
# -------------------------------
def s3_file_key(base_prefix, filename):
    today = datetime.datetime.today().strftime('%Y%m%d')
    return f"{base_prefix}{today}/{filename}"


# -------------------------------
# STEP 6: UPLOAD TO S3
# -------------------------------
def upload_df_to_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    logger.info(f"Uploaded file to S3 → {key}")


def send_email(subject, body):

    ses = boto3.client(
        'ses',
        region_name=config['email']['region'],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )

    sender = config['email']['sender']
    receivers = [r.strip() for r in config['email']['receiver'].split(",")]

    try:
        response = ses.send_email(
            Source=sender,
            Destination={"ToAddresses": receivers},
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": body}
                }
            }
        )
        logger.info("Email sent successfully.")
    except ClientError as e:
        logger.error(f"Failed to send email: {e}")


if __name__ == '__main__':

    #  Upload files to S3
    clean_key = s3_file_key(success_prefix, "clean_file.csv")
    error_key = s3_file_key(rejected_prefix, "error_file.csv")

    upload_df_to_s3(df_clean, clean_key)
    upload_df_to_s3(df_error, error_key)

    logger.info("Files uploaded successfully to S3.")

    # -----------------------------
    # EMAIL NOTIFICATION
    # -----------------------------
    today = datetime.datetime.today().strftime('%Y-%m-%d')

    if df.empty:
        # CASE: NO INCOMING FILES
        subject = f"validation email {today}"
        body = f"No incoming files found for {today}."
        send_email(subject, body)

    else:
        # CASE: FILES FOUND
        total = len(df)
        passed = len(df_clean)
        failed = len(df_error)

        subject = f"validation email {today}"

        body = (
            f"Total {total} incoming files processed.\n"
            f"{passed} files passed validation\n"
            f"{failed} files failed validation."
        )

        send_email(subject, body)

