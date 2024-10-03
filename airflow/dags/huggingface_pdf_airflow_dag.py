from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys


# Add the parent directory of 'modules' to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'modules')))
# Import the functions from the previous script
from pipeline import process_pdfs_and_upload
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'huggingface_pdf_pipeline',
    default_args=default_args,
    description='Pipeline to download, extract, and upload PDFs using Azure AI and Hugging Face',
    schedule_interval=timedelta(days=1),
) as dag:

    # Define tasks
    process_pdfs_task = PythonOperator(
        task_id='process_pdfs_task',
        python_callable=process_pdfs_and_upload,
    )
