# MySQL => s3 => Redshift(Incremental Update 방식)
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime
from datetime import timedelta


dag = DAG(
    dag_id="mysql_to_redshift_v2",
    start_date=datetime(2022, 8, 24),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)

schema, table = "dmhuh1003", "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

mysql_to_s3_nps = SqlToS3Operator(
    task_id="mysql_to_s3_nps",
    query="SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}');",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    sql_conn_id="mysql_conn_id",
    aws_conn_id="aws_conn_id",
    verify=False,
    replace=True,
    pd_kwargs={"index": False, "header": False},
    dag=dag,
)

create_redshift_nps = SQLExecuteQueryOperator(
    task_id="create_redshift_nps",
    conn_id="redshift_dev_db",
    sql=f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id INT NOT NULL PRIMARY KEY,
        created_at TIMESTAMP,
        score SMALLINT
        );
    """,
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id="s3_to_redshift_nps",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    schema=schema,
    table=table,
    copy_options=["csv"],
    method="UPSERT",
    upsert_keys=["id"],
    redshift_conn_id="redshift_dev_db",
    aws_conn_id="aws_conn_id",
    dag=dag,
)

mysql_to_s3_nps >> create_redshift_nps >> s3_to_redshift_nps
