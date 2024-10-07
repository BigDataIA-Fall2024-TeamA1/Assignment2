import os
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF for PDF extraction
import json
import base64
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
    ComplexField
)
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Hugging Face API Token
huggingface_token = os.getenv('HuggingFace_API_KEY')

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME_1 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_1')  # Container 1 for PDFs
AZURE_CONTAINER_NAME_2 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_2')  # Container 2 for extracted text
AZURE_CONTAINER_NAME_3 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_3')  # Container 3 for extracted content using PyMuPDF

# Azure Cognitive Search Configuration
AZURE_SEARCH_SERVICE_NAME = os.getenv('AZURE_SEARCH_SERVICE_NAME')
AZURE_SEARCH_ADMIN_API_KEY = os.getenv('AZURE_SEARCH_ADMIN_API_KEY')
AZURE_SEARCH_QUERY_API_KEY = os.getenv('AZURE_SEARCH_QUERY_API_KEY')
AZURE_SEARCH_INDEX_NAME = os.getenv('AZURE_SEARCH_INDEX_NAME')

# Azure Form Recognizer Configuration
AZURE_FORM_RECOGNIZER_ENDPOINT = os.getenv('AZURE_FORM_RECOGNIZER_ENDPOINT')
AZURE_FORM_RECOGNIZER_KEY = os.getenv('AZURE_FORM_RECOGNIZER_KEY')

# Initialize Azure Blob Service Clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client_1 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_1)
container_client_2 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_2)
container_client_3 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_3)  # For PyMuPDF extracted content

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
    """Upload a file to Azure Blob Storage Container 1 if it doesn't already exist."""
    try:
        blob_client = container_client_1.get_blob_client(blob_name)
        
        # Check if the blob already exists
        if blob_client.exists():
            print(f"File {blob_name} already exists in Azure Blob Storage Container 1. Skipping upload.")
            return

        # If the file does not exist, upload it
        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=False)  # Prevent overwriting
        print(f"Uploaded {file_path} to Azure Blob Storage Container 1 as {blob_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to Azure Blob Storage Container 1: {e}")

def generate_blob_sas_url(container_name, blob_name, permission, expiry_hours=1):
    """Generate a SAS URL for a blob."""
    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=blob_service_client.credential.account_key,
        permission=permission,
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
    )
    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    return blob_url

def create_search_index():
    search_index_client = SearchIndexClient(
        endpoint=f"https://{AZURE_SEARCH_SERVICE_NAME}.search.windows.net",
        credential=AzureKeyCredential(AZURE_SEARCH_ADMIN_API_KEY)
    )

    # Define the index schema for a generic PDF storage
    fields = [
        SimpleField(name="metadata_storage_path", type=SearchFieldDataType.String, key=True),  # Unique identifier for each document
        SearchableField(name="content", type=SearchFieldDataType.String, analyzer_name="en.lucene"),  # Full-text search of PDF content
        SimpleField(name="metadata_storage_name", type=SearchFieldDataType.String, filterable=True),  # File name
        SimpleField(name="metadata_title", type=SearchFieldDataType.String, searchable=True),  # Title of the document
        SimpleField(name="metadata_author", type=SearchFieldDataType.String, searchable=True),  # Author name
        ComplexField(name="pages", fields=[
            SimpleField(name="page_number", type=SearchFieldDataType.Int32, filterable=True),
            SearchableField(name="text", type=SearchFieldDataType.String),  # Text content of the page
            ComplexField(name="images", fields=[
                SimpleField(name="image_index", type=SearchFieldDataType.Int32),
                SearchableField(name="base64_data", type=SearchFieldDataType.String),  # Base64 encoded image data
                SimpleField(name="description", type=SearchFieldDataType.String)  # Description of the image
            ])
        ]),
        SimpleField(name="document_type", type=SearchFieldDataType.String, filterable=True),  # Type of document (e.g., invoice, book, etc.)
        ComplexField(name="tables", fields=[
            SimpleField(name="table_index", type=SearchFieldDataType.Int32),
            SearchableField(name="table_content", type=SearchFieldDataType.String)  # Text content of tables
        ])
    ]
    index = SearchIndex(name=AZURE_SEARCH_INDEX_NAME, fields=fields)

    # Create the index if it doesn't exist
    try:
        result = search_index_client.get_index(AZURE_SEARCH_INDEX_NAME)
        print(f"Index '{AZURE_SEARCH_INDEX_NAME}' already exists.")
    except Exception:
        print(f"Creating index '{AZURE_SEARCH_INDEX_NAME}'.")
        search_index_client.create_index(index)

def extract_content_with_azure_ai(pdf_path, blob_name):
    """Extract content from a PDF using Azure Form Recognizer page-by-page and upload to Azure Blob Storage Container 2."""
    document_analysis_client = DocumentAnalysisClient(
        endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
        credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
    )

    extracted_data = {
        "document_title": os.path.basename(pdf_path),
        "pages": []
    }

    try:
        # Open the PDF file using PyMuPDF to process it page by page
        doc = fitz.open(pdf_path)
        
        for page_num in range(doc.page_count):
            # Extract page-by-page to reduce memory overhead and handle large, image-heavy documents
            page = doc.load_page(page_num)

            # Convert the page to bytes
            page_bytes = page.get_pixmap().tobytes("png")

            # Use Azure AI to analyze the document's content
            poller = document_analysis_client.begin_analyze_document("prebuilt-layout", page_bytes)
            result = poller.result()

            # Extracting lines from the page
            page_data = {
                "page_number": page_num + 1,
                "lines": []
            }

            # Loop through the page and add text lines to `page_data`
            for line in result.pages[0].lines:
                page_data["lines"].append(line.content)

            extracted_data["pages"].append(page_data)
            print(f"Processed page {page_num + 1} of {doc.page_count}")

        # Convert the entire extracted data to JSON format
        json_content = json.dumps(extracted_data, indent=4)
        json_blob_name = f"{os.path.splitext(blob_name)[0]}_azure_extracted.json"

        # Upload the JSON file to Azure Blob Storage Container 2
        blob_client = container_client_2.get_blob_client(json_blob_name)
        blob_client.upload_blob(json_content, overwrite=True)
        print(f"Uploaded Azure AI extracted content to Azure Blob Storage Container 2 as {json_blob_name}")

    except Exception as e:
        print(f"Error extracting or uploading content from Azure Form Recognizer: {e}")
    finally:
        doc.close()

def extract_content_with_pymupdf(pdf_path):
    """Extract text and images from a PDF using PyMuPDF (fitz) and structure them in a single JSON document."""
    doc = fitz.open(pdf_path)
    extracted_data = {"document_title": os.path.basename(pdf_path), "pages": []}

    for page_num in range(doc.page_count):
        page = doc[page_num]
        page_data = {
            "page_number": page_num + 1,
            "text": page.get_text("text"),  # Extract all text on the page
            "images": []
        }

        # Extract images and encode them in base64
        image_list = page.get_images(full=True)
        for image_index, img in enumerate(image_list):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            image_base64 = base64.b64encode(image_bytes).decode("utf-8")
            image_info = {
                "image_index": image_index + 1,
                "base64_data": image_base64,
                "description": f"Image on page {page_num + 1}, index {image_index + 1}"
            }
            page_data["images"].append(image_info)

        extracted_data["pages"].append(page_data)

    doc.close()
    return extracted_data

def upload_pymupdf_extracted_content(blob_name, extracted_data):
    """Upload extracted content (text and images) to Azure Blob Storage Container 3 as a single JSON document."""
    try:
        # Convert extracted data to JSON format
        json_content = json.dumps(extracted_data, indent=4)
        json_blob_name = f"{os.path.splitext(blob_name)[0]}_pymupdf_extracted.json"
        
        # Upload to Azure Blob Storage Container 3
        blob_client = container_client_3.get_blob_client(json_blob_name)
        blob_client.upload_blob(json_content, overwrite=True)
        print(f"Uploaded PyMuPDF extracted content to Azure Blob Storage Container 3 as {json_blob_name}")
        
    except Exception as e:
        print(f"Error uploading extracted content to Azure Blob Storage Container 3: {e}")

def process_pdfs_and_upload():
    """Download PDFs from Hugging Face, upload to Azure Container 1, extract with Azure AI Document Intelligence, and upload to Container 2."""
    
    # Iterate over Hugging Face URLs
    for huggingface_url in huggingface_urls:
        pdf_urls = get_pdf_urls_from_huggingface(huggingface_url)
        if not pdf_urls:
            print(f"No PDF files found at {huggingface_url}.")
            continue

        # Process each PDF URL
        for pdf_url in pdf_urls:
            file_name = pdf_url.split('/')[-1]
            local_path = os.path.join(download_dir, file_name)

            # Step 1: Download the PDF from Hugging Face
            download_file(pdf_url, local_path)

            # Check if file was downloaded
            if os.path.exists(local_path):

                # Step 2: Upload PDF to Azure Container 1 (to maintain an original copy)
                upload_to_azure_container_1(local_path, file_name)

                # Step 3: Extract content using Azure AI Document Intelligence and upload to Container 2
                extract_content_with_azure_ai(local_path, file_name)

                # Step 4: Extract content using PyMuPDF and upload to Container 3
                extracted_data_pymupdf = extract_content_with_pymupdf(local_path)
                upload_pymupdf_extracted_content(file_name, extracted_data_pymupdf)

                # Step 5: Clean up local file
                os.remove(local_path)
            else:
                print(f"File {local_path} does not exist after download. Skipping.")

if __name__ == "__main__":
    # Initialize the search index
    create_search_index()
    
    # Process PDFs and upload them
    process_pdfs_and_upload()
