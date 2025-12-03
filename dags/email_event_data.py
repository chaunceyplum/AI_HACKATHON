from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import os
from dotenv import load_dotenv
import uuid
from faker import Faker

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# -------------------------------
# SAFE JSON FETCH WRAPPER
# -------------------------------
def fetch_json_safe(url, params):
    """Fetch JSON from Mockaroo with logging + safe parsing."""
    resp = requests.get(url, params=params)

    print("Mockaroo STATUS:", resp.status_code)
    print("Mockaroo TEXT (first 400):", resp.text[:400])

    if resp.status_code != 200:
        raise ValueError(f"Mockaroo returned HTTP {resp.status_code}: {resp.text}")

    try:
        return resp.json()
    except Exception as e:
        raise ValueError(f"Invalid JSON returned by Mockaroo: {resp.text}") from e


# -------------------------------
# STITCHED EVENTS
# -------------------------------

fake = Faker()

# -------------------------------
# EMAIL EVENTS
# -------------------------------

def generate_email_events(**context):
    """Generate realistic email events for CRM profiles."""

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

    crm_table = catalog.load_table("aihackathon.crm_profiles")
    crm_df = crm_table.scan().to_arrow().to_pandas()

    sample_size = random.randint(100, 250)
    sampled = crm_df.sample(n=sample_size).reset_index(drop=True)

    # UTM → Campaign ID lookup
    campaign_map = {
        "spring_sale": "112342",
        "black_friday": "112343",
        "holiday_special": "112344",
        "new_arrivals": "112345",
        "weekly_newsletter": "112346",
    }

    all_events = []

    for _, c in sampled.iterrows():
        events = []

        profile_id = str(c["profile_id"])
        email_id = str(c["email"])

        utm = random.choice(list(campaign_map.keys()))
        campaign_id = campaign_map[utm]

        # Always send DELIVERED
        delivered_ts = fake.date_time_between(start_date="-2w", end_date="now")

        delivered_event = {
            "event_id": str(uuid.uuid4()),
            "profile_id": profile_id,
            "email": email_id,
            "campaign_id": campaign_id,
            "event_type": "email_delivered",
            "event_timestamp": delivered_ts.isoformat(),
            "email_subject": fake.sentence(),
            "email_sender": "noreply@tapcxm.com",
            "utm_campaign": utm,
        }
        events.append(delivered_event)

        # Only <50% OPEN the email
        if random.random() < 0.40:  # 40% open rate
            opened_ts = delivered_ts + timedelta(minutes=random.randint(1, 120))
            open_event = {
                "event_id": str(uuid.uuid4()),
                "profile_id": profile_id,
                "email_id": email_id,
                "campaign_id": campaign_id,
                "event_type": "email_opened",
                "event_timestamp": opened_ts.isoformat(),
                "email_subject": delivered_event["email_subject"],
                "email_sender": delivered_event["email_sender"],
                "utm_campaign": utm,
            }
            events.append(open_event)

            # Only a fraction click (e.g., 5–15% of openers)
            if random.random() < 0.10:  # 10% click-through rate from opens
                clicked_ts = opened_ts + timedelta(minutes=random.randint(1, 60))
                click_event = {
                    "event_id": str(uuid.uuid4()),
                    "profile_id": profile_id,
                    "email_id": email_id,
                    "campaign_id": campaign_id,
                    "event_type": "email_clicked",
                    "event_timestamp": clicked_ts.isoformat(),
                    "email_subject": delivered_event["email_subject"],
                    "email_sender": delivered_event["email_sender"],
                    "utm_campaign": utm,
                }
                events.append(click_event)

        all_events.append(pd.DataFrame(events))

    df = pd.concat(all_events, ignore_index=True)
    print(f"Generated {len(df)} email events")

    arrow_table = pa.Table.from_pandas(df)
    table_name = "aihackathon.email_events"
    # catalog.create_table(table_name, schema=arrow_table.schema)s

    table = catalog.load_table(table_name)
    table.append(arrow_table)

    print(f"Wrote {len(df)} events to → {table_name}")



# -------------------------------
# DAG
# -------------------------------
with DAG(
    'email_event_data_workflow',
    default_args=default_args,
    description='Stitch CRM profiles + Generate prospect events into Iceberg',
    schedule='*/5 * * * *',
    catchup=False,
) as dag:



    email_events = PythonOperator(
        task_id='generate_email_events',
        python_callable=generate_email_events,
    )

    email_events
