import os
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Hugging Face API Token
huggingface_token = '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'

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_NAME_1')  # Container 1 for storing PDFs

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)

# Hugging Face GAIA dataset URLs for validation and test
huggingface_urls = [
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/validation",
    "https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/test"
]

# Supported file type - PDF only
SUPPORTED_FILE_TYPE = '.pdf'

# Local directory for downloading files
download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
os.makedirs(download_dir, exist_ok=True)  # Create downloads folder if it doesn't exist

def get_pdf_urls_from_huggingface(huggingface_url):
    """Retrieve all PDF download URLs from the Hugging Face page."""
    headers = {
        "Authorization": f"Bearer {huggingface_token}"
    }
    
    response = requests.get(huggingface_url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to retrieve the page: {huggingface_url}, status code: {response.status_code}")
        return []
    
    # Parse the HTML page
    soup = BeautifulSoup(response.text, 'html.parser')
    pdf_urls = []
    
    # Find all <li> elements containing <a> tags, extract href attributes
    for li in soup.find_all('li'):
        link = li.find('a', href=True)
        if link:
            href = link.get('href')
            
            # Check if the href is a PDF file
            if href.endswith(SUPPORTED_FILE_TYPE):
                full_url = f"https://huggingface.co{href}".replace('/blob/', '/resolve/')
                pdf_urls.append(full_url)
                print(f"Found PDF URL: {full_url}")  # Debug: print matching file URLs
    
    if not pdf_urls:
        print(f"No PDF files found on {huggingface_url}.")
    
    return pdf_urls

def download_file(url, local_path):
    """Download file to local storage."""
    headers = {
        "Authorization": f"Bearer {huggingface_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {url} to {local_path}")
        else:
            print(f"Failed to download {url}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def upload_to_azure(file_path, blob_name):
    """Upload file to Azure Blob Storage (Container 1)."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_path} to Azure Blob as {blob_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to Azure Blob: {e}")

def process_pdfs_and_upload():
    """Download PDFs from Hugging Face and upload them to Azure Blob Storage."""
    for huggingface_url in huggingface_urls:
        # Step 1: Get the PDF URLs
        pdf_urls = get_pdf_urls_from_huggingface(huggingface_url)
        
        if not pdf_urls:
            print("No PDF files found to download.")
            continue
        
        # Step 2: Download and upload each PDF file
        for pdf_url in pdf_urls:
            file_name = pdf_url.split('/')[-1]  # Extract the file name from the URL
            local_path = os.path.join(download_dir, file_name)  # Local file path in the Downloads folder
            
            # Download the PDF to local
            download_file(pdf_url, local_path)
            
            # Check if the file was successfully downloaded
            if os.path.exists(local_path):
                print(f"File {local_path} successfully downloaded.")
                
                # Upload the PDF to Azure Blob Storage (Container 1)
                upload_to_azure(local_path, file_name)
                
                # Optional: Delete the local file after uploading to Azure Blob
                os.remove(local_path)
                print(f"Deleted local file: {local_path}")
            else:
                print(f"File {local_path} not found, skipping upload.")

# Execute the download and upload process
process_pdfs_and_upload()
