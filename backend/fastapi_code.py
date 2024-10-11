from fastapi import FastAPI, Form, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from jose import jwt, JWTError
from passlib.context import CryptContext
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from backend.openai_client import send_to_openai
from datetime import timedelta, datetime
import os
from io import BytesIO
import base64

# Load environment variables from .env
load_dotenv(override=True)

# SQLAlchemy setup for Azure SQL
SQLALCHEMY_DATABASE_URL = os.getenv('AZURE_SQL_CONNECTION_STRING')
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME_1 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_1')  # PDF Blob Container
AZURE_CONTAINER_NAME_2 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_2')  # Azure AI Extracted Text Blob Container
AZURE_CONTAINER_NAME_3 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_3')  # PyMuDF Extracted Text Blob Container
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client_1 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_1)
container_client_2 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_2)
container_client_3 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_3)

# JWT Configuration
SECRET_KEY = os.getenv('SECRET_KEY')
ALGORITHM = os.getenv('ALGORITHM')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', 30))
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI()

# User database model
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(150), unique=True, index=True)
    hashed_password = Column(String)

Base.metadata.create_all(bind=engine)

# Dependency: Get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Helper functions for password and JWT
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# User registration endpoint
@app.post("/register")
async def register_user(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user_exists = db.query(User).filter(User.username == username).first()
    if user_exists:
        return JSONResponse(status_code=400, content={"error": "Username already exists"})

    hashed_password = pwd_context.hash(password)
    new_user = User(username=username, hashed_password=hashed_password)
    db.add(new_user)
    
    try:
        db.commit()
        db.refresh(new_user)
        return JSONResponse(status_code=200, content={"message": "Registration successful. Please login."})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"error": "Database error"})

# User login endpoint
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)

    return JSONResponse(content={"access_token": access_token, "token_type": "bearer"})

# Main page - displays PDF list
@app.get("/main", response_class=JSONResponse)
async def main_page(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        
        blobs_list = list(container_client_1.list_blobs())
        pdfs = [blob.name for blob in blobs_list if blob.name.endswith(".pdf")]

        return {"pdfs": pdfs, "username": username}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Preview PDF endpoint
@app.post("/preview-pdf")
async def preview_pdf(pdf_name: str = Form(...), token: str = Depends(oauth2_scheme)):
    try:
        # Verify token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        
        # Fetch PDF from Azure Storage Container 1
        blob_client = container_client_1.get_blob_client(pdf_name)
        pdf_stream = blob_client.download_blob().readall()

        # Return PDF as a streaming response
        return StreamingResponse(BytesIO(pdf_stream), media_type="application/pdf")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Get PDF processed content and interact with OpenAI
@app.post("/process-pdf", response_class=JSONResponse)
async def process_pdf(pdf_name: str = Form(...), extract_method: str = Form(...), user_input: str = Form(None), token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        
        # Determine which extraction method to use
        if extract_method == "Azure AI Document Intelligence":
            container_client = container_client_2
            blob_filename = f"{pdf_name.split('.')[0]}_azure_extracted.json"
        elif extract_method == "PyMuDF":
            container_client = container_client_3
            blob_filename = f"{pdf_name.split('.')[0]}_pymupdf_extracted.json"
        else:
            raise HTTPException(status_code=400, detail="Invalid extraction method specified")

        # Get the blob content
        blob_client = container_client.get_blob_client(blob_filename)
        processed_content = blob_client.download_blob().readall().decode("utf-8")

        # If a user query is provided, get a response from OpenAI
        if user_input:
            prompt = f"Extracted Text:\n{processed_content}\n\nUser Input:\n{user_input}"
            openai_response = send_to_openai(prompt)
        else:
            openai_response = None

        # Include the original PDF preview in the response as base64
        blob_client_pdf = container_client_1.get_blob_client(pdf_name)
        pdf_stream = blob_client_pdf.download_blob().readall()
        pdf_base64 = base64.b64encode(pdf_stream).decode('utf-8')

        return {"processed_content": processed_content, "openai_response": openai_response, "pdf_preview": pdf_base64}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})