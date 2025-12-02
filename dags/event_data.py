from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import os

# TEMP ONLY — do not commit long-term
os.environ["AWS_ACCESS_KEY_ID"] = "YOUR_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "YOUR_SECRET"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def generate_stitched_events(**context):
    """
    Reads CRM customers, fetches web events, stitches them, writes to Iceberg.
    """

    # Load catalog
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

    # ----- Read & sample CRM -----
    crm_table = catalog.load_table("aihackathon.crm_profiles")
    crm_df = crm_table.scan().to_arrow().to_pandas()

    sample_size = random.randint(100, 300)
    sampled_customers = crm_df.sample(n=sample_size).reset_index(drop=True)

    print(f"Sampled {sample_size} CRM profiles")

    # ----- Fetch Mockaroo web events -----
    url = "https://api.mockaroo.com/api/7b6fb2c0"
    params = {"count": 1000, "key": "ab78c110"}
    events_df = pd.DataFrame(requests.get(url, params=params).json())

    print(f"Fetched {len(events_df)} web events")

    # ----- Stitch CRM profile_ids -----
    events_df["crm_profile_id"] = (
        sampled_customers["profile_id"]
        .sample(n=len(events_df), replace=True)
        .reset_index(drop=True)
    )

    # ----- Write stitched results -----
    stitched_arrow = pa.Table.from_pandas(events_df)
    table_name = "aihackathon.web_events"

    try:
        table = catalog.load_table(table_name)
        table.append(stitched_arrow)
    except:
        catalog.create_table(table_name, schema=stitched_arrow.schema)
        table = catalog.load_table(table_name)
        table.append(stitched_arrow)

    print(f"Wrote {len(events_df)} stitched events to {table_name}")


def generate_prospect_events(**context):
    """
    Fetches Mockaroo web events and loads directly into Iceberg
    as PROSPECT events (no CRM stitching).
    """

    # Load catalog
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

    # ----- Fetch Mockaroo web events -----
    url = "https://api.mockaroo.com/api/7b6fb2c0"
    params = {"count": 1000, "key": "ab78c110"}
    events_df = pd.DataFrame(requests.get(url, params=params).json())

    print(f"Fetched {len(events_df)} raw PROSPECT events")

    # ----- Convert to Arrow -----
    arrow_table = pa.Table.from_pandas(events_df)

    # ----- Write to Iceberg -----
    table_name = "aihackathon.prospect_web_events"

    try:
        table = catalog.load_table(table_name)
        print("Appending to existing table: prospect_web_events")
        table.append(arrow_table)
    except:
        print("Creating new table: prospect_web_events")
        catalog.create_table(table_name, schema=arrow_table.schema)
        table = catalog.load_table(table_name)
        table.append(arrow_table)

    print(f"Wrote {len(events_df)} prospect events to {table_name}")


with DAG(
    'event_data_workflow',
    default_args=default_args,
    description='Stitch CRM profiles + Generate prospect events into Iceberg',
    schedule='*/15 * * * *',
    catchup=False,
) as dag:

    stitched = PythonOperator(
        task_id='generate_stitched_web_events',
        python_callable=generate_stitched_events,
    )

    prospect = PythonOperator(
        task_id='generate_prospect_web_events',
        python_callable=generate_prospect_events,
    )

    stitched >> prospect
