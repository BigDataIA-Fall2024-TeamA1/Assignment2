import os
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF for PDF extraction
import json
import base64
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
AZURE_CONTAINER_NAME_3 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_3')  # Container 3 for extracted content using PyMuPDF

# Azure AI Document Intelligence Configuration
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

# Initialize the DocumentAnalysisClient
document_analysis_client = DocumentAnalysisClient(
    endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
    credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
)

def extract_document_content(pdf_path):
    """Extract content from a PDF using Azure AI Document Intelligence's prebuilt-document model, splitting if >4MB."""
    file_size = os.path.getsize(pdf_path)
    
    if file_size <= 4 * 1024 * 1024:
        # If file is under 4MB, process directly
        with open(pdf_path, "rb") as file:
            return process_with_document_analysis(file)
    else:
        # Split into chunks and process each chunk separately
        doc = fitz.open(pdf_path)
        extracted_data = {"document_title": os.path.basename(pdf_path), "pages": []}

        # Iterate through pages in 4MB chunks
        chunk_pages = []
        chunk_size = 0

        for page_num in range(doc.page_count):
            page = doc[page_num]
            chunk_pages.append(page)
            chunk_size += len(page.get_text("text").encode('utf-8'))  # Estimate size of text content

            if chunk_size >= 4 * 1024 * 1024 or page_num == doc.page_count - 1:
                # Process the chunk if it reached ~4MB or last page
                extracted_data.update(process_chunk_pages(chunk_pages))
                chunk_pages = []
                chunk_size = 0

        doc.close()
        return extracted_data

def process_with_document_analysis(file):
    """Process a file with Azure Document Analysis Client and return extracted data."""
    try:
        poller = document_analysis_client.begin_analyze_document("prebuilt-document", document=file)
        result = poller.result()
    except Exception as e:
        print(f"Error with 'prebuilt-document': {e}. Trying 'prebuilt-read' model...")
        file.seek(0)  # Reset file pointer for retry
        poller = document_analysis_client.begin_analyze_document("prebuilt-read", document=file)
        result = poller.result()

    extracted_data = {"pages": []}
    for page in result.pages:
        page_data = {
            "page_number": page.page_number,
            "lines": [line.content for line in page.lines],
            "tables": [],
            "images": []
        }
        if hasattr(page, 'tables'):
            for table in page.tables:
                table_data = [{"row": cell.row_index, "column": cell.column_index, "content": cell.content}
                              for cell in table.cells]
                page_data["tables"].append({"table": table_data})
        if hasattr(page, 'selection_marks'):
            for selection_mark in page.selection_marks:
                if selection_mark.state == "selected":
                    page_data["images"].append("Selection Mark")
        extracted_data["pages"].append(page_data)
    
    return extracted_data

def process_chunk_pages(pages):
    """Convert chunk of pages into a single temporary PDF, analyze, and return extracted data."""
    temp_pdf_path = "temp_chunk.pdf"
    extracted_data = {"pages": []}

    # Save chunk pages to temp PDF
    temp_pdf = fitz.open()
    for page in pages:
        temp_pdf.insert_pdf(page.parent, from_page=page.number, to_page=page.number)
    temp_pdf.save(temp_pdf_path)
    temp_pdf.close()

    # Extract text from temp PDF
    with open(temp_pdf_path, "rb") as file:
        chunk_data = process_with_document_analysis(file)
    os.remove(temp_pdf_path)

    # Append chunk data to extracted_data
    extracted_data["pages"].extend(chunk_data["pages"])
    return extracted_data


def upload_extracted_content_to_container_2(blob_name, extracted_data):
    """Upload extracted content as JSON to Azure Blob Storage Container 2."""
    try:
        # Convert the extracted data to JSON format
        json_content = json.dumps(extracted_data, indent=4)
        json_blob_name = f"{os.path.splitext(blob_name)[0]}_extracted.json"

        # Upload to Azure Blob Storage Container 2
        blob_client = container_client_2.get_blob_client(json_blob_name)
        blob_client.upload_blob(json_content, overwrite=True)
        print(f"Uploaded extracted content to Azure Blob Storage Container 2 as {json_blob_name}")

    except Exception as e:
        print(f"Error uploading extracted content to Azure Blob Storage Container 2: {e}")

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
    """Download PDFs from Hugging Face, upload to Azure Container 1, extract text, and upload to Container 2 and 3."""
    
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
                
                # Step 2: Check file size and upload if necessary
                file_size = os.path.getsize(local_path)
                if file_size > MAX_FILE_SIZE_BYTES:
                    print(f"File {file_name} is too large ({file_size} bytes). Skipping extraction.")
                    upload_to_azure_container_1(local_path, file_name)
                    os.remove(local_path)
                    continue
                
                # Step 3: Upload PDF to Azure Container 1
                upload_to_azure_container_1(local_path, file_name)

                # Step 4: Extract text using Azure AI Document Intelligence
                print(f"Extracting text from {local_path} using Azure AI Document Intelligence...")
                extracted_data_azure = extract_document_content(local_path)
                upload_extracted_content_to_container_2(file_name, extracted_data_azure)

                # Step 5: Extract content (text and images) from PDF using PyMuPDF
                print(f"Extracting content from {local_path} using PyMuPDF...")
                extracted_data_pymupdf = extract_content_with_pymupdf(local_path)
                upload_pymupdf_extracted_content(file_name, extracted_data_pymupdf)

                # Clean up local file
                os.remove(local_path)
            else:
                print(f"File {local_path} does not exist after download. Skipping.")


if __name__ == "__main__":
    process_pdfs_and_upload()
