from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from bs4 import BeautifulSoup
import json

DEFAULT_ARGS = {
    "owner": "student_researcher",
    "start_date": datetime(2026, 4, 1),
}


def parse_adzuna_to_silver(**kwargs):
    s3_hook = S3Hook(aws_conn_id="garage_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="lakehouse-silver")

    keys = s3_hook.list_keys(bucket_name="bronze", prefix="adzuna/")
    latest_key = sorted(keys)[-1]

    raw_content = s3_hook.read_key(latest_key, bucket_name="bronze")
    data = json.loads(raw_content)

    for job in data.get("results", []):
        insert_query = """
            INSERT INTO silver.adzuna (
                id, adref, company_display, title, created, 
                category_tag, category_label, location_area, location_display, 
                longitude, latitude, salary_max, salary_min, 
                description, redirect_url, contract_time, contract_type, 
                salary_is_predicted
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        params = (
            job.get("id"),
            job.get("adref"),
            job.get("company", {}).get("display_name"),
            job.get("title"),
            job.get("created"),
            job.get("category", {}).get("tag"),
            job.get("category", {}).get("label"),
            " | ".join(job.get("location", {}).get("area", [])),
            job.get("location", {}).get("display_name"),
            job.get("longitude"),
            job.get("latitude"),
            job.get("salary_max"),
            job.get("salary_min"),
            job.get("description"),
            job.get("redirect_url"),
            job.get("contract_time"),
            job.get("contract_type"),
            job.get("salary_is_predicted"),
        )
        pg_hook.run(insert_query, parameters=params)


def parse_linkedin_to_silver(**kwargs):
    s3_hook = S3Hook(aws_conn_id="garage_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="lakehouse-silver")

    keys = s3_hook.list_keys(bucket_name="bronze", prefix="linkedin/")
    latest_key = sorted(keys)[-1]
    raw_html = s3_hook.read_key(latest_key, bucket_name="bronze")

    soup = BeautifulSoup(raw_html, "html.parser")
    job_cards = soup.find_all("div", class_="job-search-card")

    for card in job_cards:
        job_id = card.get("data-entity-urn", "").split(":")[-1]

        link_tag = card.find("a", class_="base-card__full-link")
        job_url = link_tag["href"] if link_tag else None

        title = card.find("h3", class_="base-search-card__title").get_text(strip=True)
        company_tag = card.find("a", class_="hidden-nested-link")
        company = company_tag.get_text(strip=True) if company_tag else None

        location = card.find("span", class_="job-search-card__location").get_text(
            strip=True
        )

        time_tag = card.find("time")
        posted_at = time_tag["datetime"] if time_tag else None

        insert_query = """
            INSERT INTO silver.linkedin (job_id, url, title, company, location, posted_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO NOTHING;
        """
        pg_hook.run(
            insert_query,
            parameters=(job_id, job_url, title, company, location, posted_at),
        )


with DAG(
    "bronze_to_silver_processing",
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="parse_adzuna_to_silver", python_callable=parse_adzuna_to_silver
    )

    t2 = PythonOperator(
        task_id="parse_linkedin_to_silver", python_callable=parse_linkedin_to_silver
    )

    t1
    t2
