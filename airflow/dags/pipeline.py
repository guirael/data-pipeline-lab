from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = '/opt/airflow/dbt/pipeline_lab'

def _upload_local_file_to_gcs():
    bucket_name = 'lab_xert'
    source_file_path = '/opt/airflow/upload/covid.csv'
    destination_blob_name = 'raw/covid.csv'

    hook = GCSHook(gcp_conn_id='google_cloud_lab')
    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob_name,
        filename=source_file_path
    )
    

with DAG(
    dag_id='pipeline_covid_data',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_upload = PythonOperator(
        task_id='upload_local_file_to_gcs',
        python_callable=_upload_local_file_to_gcs
    )

    task_load_raw = GCSToBigQueryOperator(
        task_id='gcs_to_bq_raw',
        gcp_conn_id='google_cloud_lab',
        bucket='lab_xert',
        source_objects=['raw/covid.csv'],
        destination_project_dataset_table='laboratorio-489120.ds_ex_lab.raw_covid',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[
            {'name': 'Date_Day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Date_Month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Date_Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Data_Cases', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Data_Deaths', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Location_Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Location_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Data_Population', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Location_Continent', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Data_Rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
    )

    task_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run -s staging'
    )

    task_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run -s intermediate'
    )

    task_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run -s marts'
    )

    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test'
    )

    task_upload >> task_load_raw >> task_staging >> task_intermediate >> task_marts >> task_dbt_test
