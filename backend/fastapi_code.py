
from fastapi import FastAPI, Form, Depends, HTTPException, Request, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse, HTMLResponse,RedirectResponse
from starlette.templating import Jinja2Templates
from jose import jwt, JWTError
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import timedelta, datetime
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from backend.openai_client import send_to_openai

# Load environment variables from .env
load_dotenv()

# SQLAlchemy setup for Azure SQL
SQLALCHEMY_DATABASE_URL = os.getenv('AZURE_SQL_CONNECTION_STRING')
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME_1 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_1')  # PDF Blob Container
AZURE_CONTAINER_NAME_2 = os.getenv('AZURE_STORAGE_CONTAINER_NAME_2')  # Extracted Text Blob Container

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client_1 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_1)
container_client_2 = blob_service_client.get_container_client(AZURE_CONTAINER_NAME_2)

# JWT Configuration
SECRET_KEY = os.getenv('SECRET_KEY')
ALGORITHM = os.getenv('ALGORITHM')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', 30))
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Templating setup
templates = Jinja2Templates(directory="templates")

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

# Get current user from cookie (JWT)
def get_current_user_from_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

# User registration
@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.post("/register")
async def register_user(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user_exists = db.query(User).filter(User.username == username).first()
    if user_exists:
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_password = pwd_context.hash(password)
    new_user = User(username=username, hashed_password=hashed_password)
    db.add(new_user)
    
    try:
        db.commit()
        db.refresh(new_user)
        print(f"New user {new_user.username} is added.")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Database error")
    
    return RedirectResponse("/login", status_code=302)

# User login and JWT token creation
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)

    # 返回 JWT 令牌为 JSON 格式，而不是重定向
    return JSONResponse(content={"access_token": access_token, "token_type": "bearer"})


# Main page - displays PDF list
@app.get("/main")
async def main_page(token: str = Depends(oauth2_scheme)):
    try:
        username = get_current_user_from_token(token)
        print(f"Authenticated user: {username}")

        # 获取 Blob 列表并打印调试信息
        print("Listing blobs in container...")
        blobs_list = list(container_client_1.list_blobs())
        for blob in blobs_list:
            print(f"Blob found: {blob.name}")

        pdfs = [blob.name for blob in blobs_list if blob.name.endswith(".pdf")]
        print(f"Found PDFs: {pdfs}")

        return {"pdfs": pdfs}
    except Exception as e:
        print(f"Error listing PDFs: {e}")
        raise HTTPException(status_code=500, detail="Error fetching PDFs from Azure Blob Storage")


# Get PDF processed content and interact with OpenAI
@app.post("/process-pdf")
async def process_pdf(pdf_name: str = Form(...), user_input: str = Form(...), token: str = Depends(oauth2_scheme)):
    try:
        # 获取当前登录的用户
        username = get_current_user_from_token(token)
        print(f"Authenticated user: {username}")

        # 获取与PDF文件相关的处理后JSON文件名
        blob_filename = f"{pdf_name.split('.')[0]}_azure_extracted.json"
        print(f"Attempting to fetch blob: {blob_filename}")

        # 从Azure Blob Storage中获取处理后的JSON文件内容
        blob_client = container_client_2.get_blob_client(blob_filename)
        print(f"Downloading content from blob: {blob_filename}")
        processed_content = blob_client.download_blob().readall().decode("utf-8")
        print(f"Successfully downloaded blob content, content length: {len(processed_content)} characters")

        # 打印用户在对话框中输入的文本
        print(f"User input: {user_input}")

        # 合并处理后的JSON文件内容与用户输入的文本，生成OpenAI的prompt
        prompt = f"Extracted Text:\n{processed_content}\n\nUser Input:\n{user_input}"
        print(f"Prepared prompt for OpenAI: {prompt[:500]}...")  # 只打印前500个字符，避免过长输出

        # 调用OpenAI API，发送prompt
        openai_response = send_to_openai(prompt)

        # 打印OpenAI的响应
        print(f"OpenAI response: {openai_response}")

        # 返回处理后的PDF内容和OpenAI的响应
        return {"processed_content": processed_content, "openai_response": openai_response}

    except Exception as e:
        # 捕捉并打印异常信息
        print(f"Error processing PDF or communicating with OpenAI: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing PDF or communicating with OpenAI: {str(e)}")
