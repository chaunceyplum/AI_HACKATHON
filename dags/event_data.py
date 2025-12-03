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

def generate_stitched_events(**context):
    """
    Generate 10–50 events per customer using Faker, keeping session/profile/crm ids consistent.
    """


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

    # Load CRM profiles
    crm_table = catalog.load_table("aihackathon.crm_profiles")
    crm_df = crm_table.scan().to_arrow().to_pandas()

    sample_size = random.randint(100, 300)
    sampled_customers = crm_df.sample(n=sample_size).reset_index(drop=True)
    print(f"Sampled {sample_size} CRM profiles")

    all_events = []

    for _, customer in sampled_customers.iterrows():
        num_events = random.randint(10, 50)
        session_id = str(uuid.uuid4())
        profile_id = str(customer["profile_id"])
        crm_profile_id = profile_id

        customer_events = []
        for _ in range(num_events):
            event = {
                "event_id": str(uuid.uuid4()),
                "profile_id": profile_id,
                "session_id": session_id,
                "crm_profile_id": crm_profile_id,
                "event_type": random.choice(["click", "view", "purchase"]),
                "event_timestamp": fake.date_between(start_date="-1y", end_date="today").strftime("%m/%d/%Y"),
                "page_url": random.choice(["/contact", "/", "/cart", "/products", "/about", "/blog", "/pricing", "/faq", "/contact"]),
                "page_title": fake.sentence(nb_words=6),
                "referrer_url": fake.url(),
                "browser": fake.user_agent(),
                "os": fake.word(),
                "device_type": random.choice(["smartphone", "desktop", "tablet"]),
                "country": fake.country(),
                "region": None,
                "city": fake.city(),
                "campaign_id": str(uuid.uuid4()),
                "campaign_source": fake.word(),
                "utm_medium": random.choice(["email", "social", "ads"]),
                "utm_campaign": fake.word(),
                "product_id": str(uuid.uuid4()),
                "product_category": fake.word(),
            }
            customer_events.append(event)

        all_events.append(pd.DataFrame(customer_events))

    events_df = pd.concat(all_events, ignore_index=True)
    print(f"Generated {len(events_df)} stitched events with Faker")

    # Write to Iceberg
    arrow_table = pa.Table.from_pandas(events_df)
    table_name = "aihackathon.web_events"

    try:
        table = catalog.load_table(table_name)
        table.append(arrow_table)
    except:
        # catalog.create_table(table_name, schema=arrow_table.schema)
        table = catalog.load_table(table_name)
        table.append(arrow_table)

    print(f"Wrote {len(events_df)} stitched events to {table_name}")

# -------------------------------
# PROSPECT EVENTS
# -------------------------------
# -------------------------------
# PROSPECT EVENTS
# -------------------------------
def generate_prospect_events(**context):
    """
    Generate 10–50 prospect events per "prospect" using Faker,
    keeping session/profile/crm ids consistent.
    """

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

    # Decide how many "prospects" to generate
    num_prospects = random.randint(1000, 3000)

    all_events = []

    for _ in range(num_prospects):
        num_events = random.randint(10, 50)
        session_id = str(uuid.uuid4())
        profile_id = str(uuid.uuid4())
        crm_profile_id = profile_id  # keep consistent

        customer_events = []
        for _ in range(num_events):
            event = {
                "event_id": str(uuid.uuid4()),
                "profile_id": profile_id,
                "session_id": session_id,
                # "crm_profile_id": crm_profile_id,
                "event_type": random.choice(["click", "view", "purchase"]),
                "event_timestamp": fake.date_between(start_date="-1y", end_date="today").strftime("%m/%d/%Y"),
                "page_url": random.choice(["/contact", "/", "/cart", "/products", "/about", "/blog", "/pricing", "/faq"]),
                "page_title": fake.sentence(nb_words=6),
                "referrer_url": fake.url(),
                "browser": fake.user_agent(),
                "os": fake.word(),
                "device_type": random.choice(["smartphone", "desktop", "tablet"]),
                "country": fake.country(),
                "region": None,
                "city": fake.city(),
                "campaign_id": str(uuid.uuid4()),
                "campaign_source": fake.word(),
                "utm_medium": random.choice(["email", "social", "ads"]),
                "utm_campaign": fake.word(),
                "product_id": str(uuid.uuid4()),
                "product_category": fake.word(),
            }
            customer_events.append(event)

        all_events.append(pd.DataFrame(customer_events))

    events_df = pd.concat(all_events, ignore_index=True)
    print(f"Generated {len(events_df)} prospect events with Faker")

    # Write to Iceberg
    arrow_table = pa.Table.from_pandas(events_df)
    table_name = "aihackathon.prospect_web_events"

    try:
        table = catalog.load_table(table_name)
        print("Appending to existing table: prospect_web_events")
        table.append(arrow_table)
    except:
        # If table doesn't exist, create it
        # catalog.create_table(table_name, schema=arrow_table.schema)
        table = catalog.load_table(table_name)
        table.append(arrow_table)

    print(f"Wrote {len(events_df)} prospect events to {table_name}")



# -------------------------------
# EMAIL EVENTS
# -------------------------------
def generate_email_events(**context):
    """
    Generate email events for existing CRM profiles.
    Schema matches the one user provided.
    """

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

    all_events = []

    for _, c in sampled.iterrows():
        events = []
        num_events = random.randint(5, 20)
        
        campaign = {
            "Spring Sale":"112342", 
            "Black Friday":"112343", 
            "Holiday Special":"112344", 
            "New Arrivals":"112345", 
            "Weekly Newsletter":"112346"
        }

        profile_id = str(c["profile_id"])
        email_id = str(c["email_id"])

        for _ in range(num_events):
            event = {
                "event_id": str(uuid.uuid4()),
                "profile_id": profile_id,
                "email_id": email_id,
                "campaign_id": str(uuid.uuid4()),
                "event_type": random.choice(["email_opened", "email_clicked", "email_delivered"]),
                "event_timestamp": fake.date_between(start_date="-2w", end_date="today").strftime("%m/%d/%Y"),
                "email_subject": fake.sentence(),
                "email_sender": "noreply@tapcxm.com",
                "utm_campaign": random.choice(["spring_sale", "black_friday", "holiday_special", "new_arrivals", "weekly_newsletter"]),
            }
            events.append(event)

        all_events.append(pd.DataFrame(events))

    df = pd.concat(all_events, ignore_index=True)
    print(f"Generated {len(df)} email events")

    arrow_table = pa.Table.from_pandas(df)
    table_name = "aihackathon.email_events"

    try:
        table = catalog.load_table(table_name)
        table.append(arrow_table)
    except:
        table = catalog.load_table(table_name)
        table.append(arrow_table)

    print(f"Wrote {len(df)} email events → {table_name}")




# -------------------------------
# DAG
# -------------------------------
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
