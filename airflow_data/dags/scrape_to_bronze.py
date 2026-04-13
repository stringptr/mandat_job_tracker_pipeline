from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import os
import json
import time
import math

ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")
DB_STAGING_SERVER = os.environ.get("DB_STAGING_SERVER")

LINKEDIN_BASE_URL = Variable.get(
    "linkedin_search_url",
    default_var="https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
)

DEFAULT_ARGS = {
    "owner": "student_researcher",
    "start_date": datetime(2026, 4, 1),
}


def fetch_adzuna(**kwargs):
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise ValueError("Missing Adzuna Credentials!")

    base_url = "https://api.adzuna.com/v1/api/jobs/gb/search"
    results_per_page = 50  # Maximize each request
    all_results = []

    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": "Data Engineer",
        "max_days_old": 14,
        "results_per_page": results_per_page,
    }

    first_response = requests.get(f"{base_url}/1", params=params)
    if first_response.status_code != 200:
        return None

    data = first_response.json()
    total_jobs = data.get("count", 0)
    total_pages = math.ceil(total_jobs / results_per_page)

    max_pages_to_fetch = min(total_pages, 100)

    print(f"Total jobs found: {total_jobs}. Fetching {max_pages_to_fetch} pages...")
    all_results.extend(data.get("results", []))

    for page in range(2, max_pages_to_fetch + 1):
        print(f"Fetching page {page}...")
        response = requests.get(f"{base_url}/{page}", params=params)

        if response.status_code == 200:
            page_data = response.json()
            all_results.extend(page_data.get("results", []))
        else:
            print(f"Failed to fetch page {page}")
            break

        time.sleep(3)

    # Return a structure similar to the original so your Silver DAG doesn't break
    return {"results": all_results}


def scrape_linkedin(**kwargs):
    """Uses Airflow Variables for dynamic URL management."""
    url = f"{LINKEDIN_BASE_URL}?keywords=Data+Engineer&f_TPR=r1209600"
    response = requests.get(url)
    if response.status_code == 200:
        print("Data fetched successfully from LinkedIn.")
        return response.text
    else:
        print("Unknown error happened.")


def store_adzuna_json_to_bronze(task_instance, **kwargs):
    data = task_instance.xcom_pull(task_ids="fetch_adzuna")

    if not data:
        raise ValueError("No data found in XCom! Did the previous task return it?")

    # Airflow Connection Id you created in UI
    hook = S3Hook(aws_conn_id="garage_s3_conn")

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    object_key = f"adzuna/{timestamp}.json"

    hook.load_string(
        string_data=json.dumps(data), key=object_key, bucket_name="bronze", replace=True
    )
    print(f"Successfully landed Adzuna JSON to Garage: {object_key}")


def store_linkedin_html_to_bronze(task_instance, **kwargs):
    raw_html = task_instance.xcom_pull(task_ids="scrape_linkedin")

    if not raw_html:
        raise ValueError("No data found in XCom! Did the previous task return it?")

    hook = S3Hook(aws_conn_id="garage_s3_conn")

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    object_key = f"linkedin/{timestamp}.html"

    hook.load_string(
        string_data=str(raw_html), key=object_key, bucket_name="bronze", replace=True
    )
    print(f"Successfully landed raw LinkedIn HTML to Garage: {object_key}")


with DAG(
    "scrape_to_bronze_processing",
    default_args=DEFAULT_ARGS,
    # schedule_interval="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="fetch_adzuna", python_callable=fetch_adzuna)

    t2 = PythonOperator(task_id="scrape_linkedin", python_callable=scrape_linkedin)

    t3 = PythonOperator(
        task_id="store_adzuna_json_to_bronze",
        python_callable=store_adzuna_json_to_bronze,
    )

    t4 = PythonOperator(
        task_id="store_linkedin_html_to_bronze",
        python_callable=store_linkedin_html_to_bronze,
    )

    t1 >> t3
    t2 >> t4
