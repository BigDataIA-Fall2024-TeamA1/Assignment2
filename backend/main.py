import os
from azure.storage.blob import BlobServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

# Initialize Azure Form Recognizer Client
form_recognizer_client = DocumentAnalysisClient(
    endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
    credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
)

def list_pdfs_in_container_1():
    """List all PDF files in Azure Blob Storage Container 1."""
    print(f"Listing PDFs in Container: {AZURE_CONTAINER_NAME_1}")
    pdf_files = []
    blobs_list = container_client_1.list_blobs()
    
    for blob in blobs_list:
        if blob.name.endswith(".pdf"):  # Only PDFs
            pdf_files.append(blob.name)
            print(f"Found PDF: {blob.name}")
    
    return pdf_files

def download_pdf_from_azure(blob_name):
    """Download PDF from Azure Blob Storage (Container 1) to local directory."""
    local_path = os.path.join(os.path.expanduser("~"), "Downloads", blob_name)
    
    try:
        print(f"Downloading {blob_name} from Azure Blob...")
        with open(local_path, "wb") as pdf_file:
            download_stream = container_client_1.download_blob(blob_name)
            pdf_file.write(download_stream.readall())
        print(f"Downloaded {blob_name} to {local_path}")
        return local_path
    except Exception as e:
        print(f"Error downloading {blob_name}: {e}")
        return None

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file using Azure AI Document Intelligence."""
    with open(pdf_path, "rb") as pdf_file:
        poller = form_recognizer_client.begin_analyze_document("prebuilt-read", document=pdf_file)
        result = poller.result()
    
    extracted_text = ""
    for page in result.pages:
        for line in page.lines:
            extracted_text += line.content + "\n"
    
    return extracted_text

def upload_extracted_text_to_azure(blob_name, extracted_text):
    """Upload extracted text to Azure Blob Storage (Container 2)."""
    try:
        blob_client = container_client_2.get_blob_client(blob_name)
        blob_client.upload_blob(extracted_text, overwrite=True)
        print(f"Uploaded extracted text to Azure Blob as {blob_name}")
    except Exception as e:
        print(f"Error uploading extracted text to Azure Blob: {e}")

def process_pdfs_and_extract_text():
    """Download PDFs from Azure, extract text using Azure AI, and upload extracted text to Azure Blob (Container 2)."""
    pdf_files = list_pdfs_in_container_1()

    if not pdf_files:
        print("No PDF files found in Container 1.")
        return
    
    for pdf_file in pdf_files:
        # Step 1: Download the PDF from Azure Blob Storage (Container 1)
        local_pdf_path = download_pdf_from_azure(pdf_file)
        if not local_pdf_path:
            continue  # Skip if download failed
        
        # Step 2: Extract text from the PDF using Azure AI
        print(f"Extracting text from {local_pdf_path}...")
        extracted_text = extract_text_from_pdf(local_pdf_path)
        
        # Step 3: Upload the extracted text to Azure Blob Storage (Container 2)
        blob_name = pdf_file.replace(".pdf", "_extracted.txt")  # Naming for the extracted text file
        upload_extracted_text_to_azure(blob_name, extracted_text)
        
        # Optional: Delete the local PDF after processing
        os.remove(local_pdf_path)
        print(f"Deleted local PDF file: {local_pdf_path}")

# Execute the process
if __name__ == "__main__":
    process_pdfs_and_extract_text()
