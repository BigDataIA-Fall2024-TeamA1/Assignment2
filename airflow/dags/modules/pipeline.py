import os
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Hugging Face API Token
huggingface_token = os.getenv('HuggingFace_API_KEY')

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME_1 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_1')  # Container 1 for PDFs
AZURE_CONTAINER_NAME_2 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_2')  # Container 2 for extracted text

# Azure AI Document Intelligence Configuration
AZURE_FORM_RECOGNIZER_ENDPOINT = os.getenv('AZURE_FORM_RECOGNIZER_ENDPOINT')
AZURE_FORM_RECOGNIZER_KEY = os.getenv('AZURE_FORM_RECOGNIZER_KEY')

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client_1 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_1)
container_client_2 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_2)

# Hugging Face GAIA dataset URLs
huggingface_urls = [
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/validation",
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/test"
]

# Supported file type - PDF only
SUPPORTED_FILE_TYPE = '.pdf'

# Local directory for downloading files
download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
os.makedirs(download_dir, exist_ok=True)

# Maximum allowed file size in bytes (e.g., 10MB)
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10MB

def get_pdf_urls_from_huggingface(huggingface_url):
    """Scrape Hugging Face URL to extract PDF file URLs."""
    headers = {"Authorization": f"Bearer {huggingface_token}"}
    response = requests.get(huggingface_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve the page: {huggingface_url}, status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    pdf_urls = []

    for li in soup.find_all('li'):
        link = li.find('a', href=True)
        if link:
            href = link.get('href')
            if href.endswith(SUPPORTED_FILE_TYPE):
                full_url = f"https://huggingface.co{href}".replace('/blob/', '/resolve/')
                pdf_urls.append(full_url)
                print(f"Found PDF URL: {full_url}")

    return pdf_urls

def download_file(url, local_path):
    """Download a file from a URL to a local path."""
    headers = {"Authorization": f"Bearer {huggingface_token}"}
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {url} to {local_path}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def upload_to_azure_container_1(file_path, blob_name):
    """Upload a file to Azure Blob Storage Container 1."""
    try:
        blob_client = container_client_1.get_blob_client(blob_name)
        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_path} to Azure Blob Storage Container 1 as {blob_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to Azure Blob Storage Container 1: {e}")

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file using Azure AI Document Intelligence."""
    try:
        form_recognizer_client = DocumentAnalysisClient(
            endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
            credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
        )

        with open(pdf_path, "rb") as f:
            poller = form_recognizer_client.begin_analyze_document("prebuilt-read", document=f)
            result = poller.result()

        extracted_text = ""
        for page in result.pages:
            for line in page.lines:
                extracted_text += line.content + "\n"

        return extracted_text
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None

def upload_extracted_text_to_container_2(blob_name, extracted_text):
    """Upload extracted text to Azure Blob Storage Container 2."""
    try:
        text_blob_name = f"{os.path.splitext(blob_name)[0]}_extracted.txt"
        blob_client = container_client_2.get_blob_client(text_blob_name)
        blob_client.upload_blob(extracted_text, overwrite=True)
        print(f"Uploaded extracted text to Azure Blob Storage Container 2 as {text_blob_name}")
    except Exception as e:
        print(f"Error uploading extracted text to Azure Blob Storage Container 2: {e}")

def process_pdfs_and_upload():
    """Download PDFs from Hugging Face, upload to Azure Container 1, extract text, and upload to Container 2."""
    for huggingface_url in huggingface_urls:
        pdf_urls = get_pdf_urls_from_huggingface(huggingface_url)
        if not pdf_urls:
            print(f"No PDF files found at {huggingface_url}.")
            continue

        for pdf_url in pdf_urls:
            file_name = pdf_url.split('/')[-1]
            local_path = os.path.join(download_dir, file_name)

            # Step 1: Download the PDF from Hugging Face
            download_file(pdf_url, local_path)

            if os.path.exists(local_path):
                # Step 2: Check file size
                file_size = os.path.getsize(local_path)
                if file_size > MAX_FILE_SIZE_BYTES:
                    print(f"File {file_name} is too large ({file_size} bytes). Skipping extraction.")
                    # Optional: Upload the large PDF to Container 1 without extraction
                    upload_to_azure_container_1(local_path, file_name)
                    os.remove(local_path)
                    print(f"Deleted local file: {local_path}")
                    continue

                # Step 3: Upload the PDF to Azure Blob Storage Container 1
                upload_to_azure_container_1(local_path, file_name)

                # Step 4: Extract text from the PDF
                print(f"Extracting text from {local_path}...")
                extracted_text = extract_text_from_pdf(local_path)

                if extracted_text:
                    # Step 5: Upload the extracted text to Azure Blob Storage Container 2
                    upload_extracted_text_to_container_2(file_name, extracted_text)

                # Optional: Delete the local PDF after processing
                os.remove(local_path)
                print(f"Deleted local file: {local_path}")
            else:
                print(f"File {local_path} does not exist after download. Skipping.")

def download_pdfs(huggingface_urls, download_dir):
    pdf_urls = []
    for huggingface_url in huggingface_urls:
        urls = get_pdf_urls_from_huggingface(huggingface_url)
        pdf_urls.extend(urls)
    
    downloaded_files = []
    for pdf_url in pdf_urls:
        file_name = pdf_url.split('/')[-1]
        local_path = os.path.join(download_dir, file_name)
        download_file(pdf_url, local_path)
        downloaded_files.append(local_path)
    
    return downloaded_files

# Step 2: Upload PDF to Azure Container 1
def upload_pdfs_to_azure(downloaded_files):
    for file_path in downloaded_files:
        file_name = os.path.basename(file_path)
        upload_to_azure_container_1(file_path, file_name)

# Step 3: Extract text from PDF
def extract_texts_from_pdfs(downloaded_files, max_file_size_mb=10):
    extracted_texts = []
    for pdf_path in downloaded_files:
        # 檢查檔案大小
        file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
        if file_size_mb > max_file_size_mb:
            print(f"Skipping {pdf_path} because its size is {file_size_mb:.2f} MB, which exceeds the limit of {max_file_size_mb} MB.")
            continue

        # 提取文本
        try:
            extracted_text = extract_text_from_pdf(pdf_path)
            if extracted_text:
                extracted_texts.append((pdf_path, extracted_text))
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
    
    return extracted_texts

# Step 4: Upload extracted text to Azure Container 2
def upload_extracted_texts_to_azure(extracted_texts):
    for pdf_path, extracted_text in extracted_texts:
        file_name = os.path.basename(pdf_path)
        upload_extracted_text_to_container_2(file_name, extracted_text)

# Execute the process