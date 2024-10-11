from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add the parent directory of 'modules' to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'modules')))

# Import the functions from pipeline.py
from pipeline import download_pdfs, upload_pdfs_to_azure, extract_texts_from_pdfs, upload_extracted_texts_to_azure

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the URLs and download directory here
huggingface_urls = [
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/validation",
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/test"
]

download_dir = os.path.join(os.path.expanduser("~"), "Downloads")

# Define the DAG
with DAG(
    'huggingface_pdf_pipeline',
    default_args=default_args,
    description='Pipeline to download, extract, and upload PDFs using Azure AI and Hugging Face',
    schedule_interval=timedelta(days=1),
) as dag:

    # Task 1: Download PDFs
    download_pdfs_task = PythonOperator(
        task_id='download_pdfs_task',
        python_callable=download_pdfs,
        op_kwargs={'huggingface_urls': huggingface_urls, 'download_dir': download_dir},
    )

    # Task 2: Upload PDFs to Azure
    upload_pdfs_task = PythonOperator(
        task_id='upload_pdfs_task',
        python_callable=upload_pdfs_to_azure,
        op_kwargs={'downloaded_files': download_dir},  # This assumes the downloaded files are stored in download_dir
    )

    # Task 3: Extract text from PDFs
    extract_texts_task = PythonOperator(
        task_id='extract_texts_task',
        python_callable=extract_texts_from_pdfs,
        op_kwargs={'downloaded_files': download_dir},
    )

    # Task 4: Upload extracted texts to Azure
    upload_extracted_texts_task = PythonOperator(
        task_id='upload_extracted_texts_task',
        python_callable=upload_extracted_texts_to_azure,
        op_kwargs={'extracted_texts': download_dir},  # Assuming extracted texts are stored in download_dir
    )

    # Set up task dependencies
    download_pdfs_task >> upload_pdfs_task >> extract_texts_task >> upload_extracted_texts_task
