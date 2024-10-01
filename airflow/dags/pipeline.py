from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import boto3

# Environment variables for Hugging Face, Azure, and AWS
HUGGINGFACE_API_KEY = os.getenv('HUGGINGFACE_API_KEY')
AZURE_FORM_RECOGNIZER_ENDPOINT = os.getenv('AZURE_FORM_RECOGNIZER_ENDPOINT')
AZURE_FORM_RECOGNIZER_KEY = os.getenv('AZURE_FORM_RECOGNIZER_KEY')
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

# URLs to download from
URLS = [
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/test",
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/validation"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_extract_store_pipeline',
    default_args=default_args,
    description='Download PDFs, extract text using Azure AI Document Intelligence, and store in AWS S3',
    schedule_interval='@daily',
    catchup=False,
)

# Step 1: Download PDFs from Hugging Face URLs
def download_pdfs():
    headers = {'Authorization': f'Bearer {HUGGINGFACE_API_KEY}'}
    pdf_urls = []  # List to hold the URLs of the PDFs

    for url in URLS:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse HTML to extract PDF URLs using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            if link['href'].endswith('.pdf'):
                pdf_urls.append(f"https://huggingface.co{link['href']}")

    os.makedirs('/tmp/pdf_files', exist_ok=True)

    for pdf_url in pdf_urls:
        filename = pdf_url.split('/')[-1]
        pdf_path = f'/tmp/pdf_files/{filename}'

        response = requests.get(pdf_url, headers=headers, stream=True)
        response.raise_for_status()

        with open(pdf_path, 'wb') as file:
            file.write(response.content)

# Step 2: Upload PDFs to Azure Blob Storage
def upload_to_azure_blob():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)

    for pdf_file in os.listdir('/tmp/pdf_files'):
        file_path = os.path.join('/tmp/pdf_files', pdf_file)
        blob_client = container_client.get_blob_client(pdf_file)

        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)

# Step 3: Extract Text Using Azure AI Document Intelligence
def extract_text_from_pdf():
    document_analysis_client = DocumentAnalysisClient(
        endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
        credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
    )

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)

    extracted_texts = {}
    for pdf_file in os.listdir('/tmp/pdf_files'):
        blob_url = f"https://{AZURE_STORAGE_CONNECTION_STRING.split(';')[1].split('=')[1]}.blob.core.windows.net/{AZURE_STORAGE_CONTAINER_NAME}/{pdf_file}"
        
        poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", blob_url)
        result = poller.result()
        
        extracted_text = []
        for page in result.pages:
            for line in page.lines:
                extracted_text.append(line.content)
        
        extracted_texts[pdf_file] = "\n".join(extracted_text)
    
    # Save extracted text locally for the next step
    os.makedirs('/tmp/extracted_texts', exist_ok=True)
    for filename, text in extracted_texts.items():
        with open(f'/tmp/extracted_texts/{filename}.txt', 'w') as file:
            file.write(text)

# Step 4: Store Extracted Text in AWS S3
def store_text_in_s3():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    for text_file in os.listdir('/tmp/extracted_texts'):
        file_path = os.path.join('/tmp/extracted_texts', text_file)
        s3_client.upload_file(file_path, AWS_S3_BUCKET_NAME, text_file)

# Define the DAG tasks
download_task = PythonOperator(
    task_id='download_pdfs',
    python_callable=download_pdfs,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_azure_blob',
    python_callable=upload_to_azure_blob,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_text_from_pdf',
    python_callable=extract_text_from_pdf,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_text_in_s3',
    python_callable=store_text_in_s3,
    dag=dag,
)

# Set task dependencies
download_task >> upload_task >> extract_task >> store_task
