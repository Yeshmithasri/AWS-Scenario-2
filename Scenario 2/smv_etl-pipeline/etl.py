import os
import io
import boto3
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, DateTime
import logging

# Environment Variables
S3_BUCKET = os.environ.get("S3_BUCKET", "smv-data-bucket")
S3_PREFIX = os.environ.get("S3_PREFIX", "")
PG_HOST = os.environ.get("POSTGRES_HOST", "smv-postgres.c18qo6wkyztn.eu-north-1.rds.amazonaws.com")
PG_DB = os.environ.get("POSTGRES_DB", "smv_db")
PG_USER = os.environ.get("POSTGRES_USER", "smv_user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "smv_pass123")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")

CHUNKSIZE = 1000
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to AWS S3 and PostgreSQL
s3 = boto3.client('s3')
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}')
metadata = MetaData()

processed_data = Table(
    'processed_data', metadata,
    Column('OrderID', Integer),
    Column('Customer', String),
    Column('Product', String),
    Column('Quantity', Integer),
    Column('Price', Float),
    Column('Date', String),
    Column('ingested_at', DateTime)
)

metadata.create_all(engine)

def process_s3_csv(file_key):
    try:
        logging.info(f"Processing {file_key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        expected_cols = ['OrderID', 'Customer', 'Product', 'Quantity', 'Price', 'Date']
        df = df[[col for col in expected_cols if col in df.columns]]
        df['ingested_at'] = pd.Timestamp.utcnow()
        df.to_sql('processed_data', engine, if_exists='append', index=False, chunksize=CHUNKSIZE)
        logging.info(f"Loaded {len(df)} rows from {file_key} into Postgres")
    except Exception as e:
        logging.error(f"Failed to process {file_key}: {e}")

def main():
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            process_s3_csv(obj['Key'])

if __name__ == "__main__":
    main()
