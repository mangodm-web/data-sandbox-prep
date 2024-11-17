from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime, timedelta
import logging
import requests
import psycopg2
from requests.exceptions import RequestException


def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


@task
def extract(url):
    try:
        logging.info(f"Extracting data from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        records = response.json()
        logging.info(f"Successfully extracted {len(records)} records.")
        return records
    except RequestException as e:
        logging.error(f"Failed to fetch data from {url}. Error: {str(e)}")
        raise
    except ValueError as e:
        logging.error(f"Failed to parse JSON from {url}. Error: {str(e)}")
        raise


@task
def transform(records):
    data = []
    for record in records:
        country = record["name"]["official"]
        population = record["population"]
        area = record["area"]
        values = (country, population, area)
        data.append(values)
    logging.info(f"Transformed {len(data)} records")

    return data


@task
def load(data, schema, table):
    cur = get_redshift_connection()
    try:
        cur.execute("BEGIN;")

        drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table};"
        cur.execute(drop_table_query)

        create_table_query = f"""
        CREATE TABLE {schema}.{table} (
            name VARCHAR(128),
            population INTEGER,
            area REAL
        );
        """
        cur.execute(create_table_query)

        insert_query = f"""
            INSERT INTO {schema}.{table} (name, population, area)
            VALUES (%s, %s, %s);
        """
        cur.executemany(insert_query, data)
        cur.execute("COMMIT;")
        logging.info(f"Loaded {len(data)} records into {schema}.{table}")
    except (Exception, psycopg2.DatabaseError) as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Failed to load data. Error: {str(e)}")
        raise


with DAG(
    dag_id="countries_to_redshift",
    start_date=datetime(2023, 5, 23),
    schedule="30 6 * * 6",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
) as dag:
    schema, table = "dmhuh1003", "country"
    base_url = Variable.get("rest_countries_base_url")
    link = f"{base_url}/all"

    records = extract(link)
    transformed_data = transform(records)
    load(transformed_data, schema, table)
