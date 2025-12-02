from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, TimestampType
import pyarrow as pa
import os
os.environ["AWS_ACCESS_KEY_ID"] = "AKIAR4K4LE4XMUIPZW54"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_write_to_iceberg(**context):
    # Fetch data from Mockaroo endpoint
    url = "https://api.mockaroo.com/api/61b86990?count=3&key=ab78c110"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} records from Mockaroo")
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Configure Iceberg catalog for S3

    catalog = load_catalog(
    "default",
    **{
        "type": "glue",
        "glue.region": "us-east-1",
        "client.region": "us-east-1",
        "s3.region": "us-east-1",
        "warehouse": "s3://baisleylake/AI_HACKATHON/aihackathon/",
        "s3.endpoint": "https://s3.us-east-1.amazonaws.com",
    }
)




    
    # # Table name
    table_name = "aihackathon.crm_profiles"
    
    # Create or append to Iceberg table
    try:
        print(f"Appending to existing Iceberg table: {table_name}")
        iceberg_table = catalog.load_table(table_name)
        iceberg_table.append(table)
    except:
        # If table doesn't exist, create it
        print(f"Creating new Iceberg table: {table_name}")
        catalog.create_table(table_name, schema=table.schema)
        iceberg_table = catalog.load_table(table_name)
        iceberg_table.append(table)
    
    # print(f"Successfully wrote {len(df)} rows to Iceberg table in S3")

with DAG(
    'mockaroo_to_s3_iceberg',
    default_args=default_args,
    description='Fetch data from Mockaroo and write to S3 in Iceberg format',
    schedule='@hourly',
    catchup=False,
) as dag:
    
    fetch_and_write = PythonOperator(
        task_id='fetch_and_write_to_iceberg',
        python_callable=fetch_and_write_to_iceberg,
    )