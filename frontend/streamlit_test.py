import streamlit as st
import requests
import sys
import os

# 将项目的根目录添加到 sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 现在可以从 backend 导入模块
from backend.openai_client import send_to_openai

# API URLs
api_url_register = "http://backend:8000/register"  # 注册 URL
api_url_token = "http://backend:8000/token"  # 登录 URL
api_url_main = "http://backend:8000/main"  # 获取 PDF 列表
api_url_process = "http://backend:8000/process-pdf"  # 处理 PDF

# 选择登录或注册模式
st.title("User Authentication")

auth_mode = st.radio("Choose an option", ["Login", "Register"])

if auth_mode == "Register":
    # 用户注册
    st.header("Register a New Account")
    reg_username = st.text_input("New Username")
    reg_password = st.text_input("New Password", type="password")

    if st.button("Register"):
        reg_response = requests.post(api_url_register, data={"username": reg_username, "password": reg_password})
        
        if reg_response.status_code == 302:
            st.success("Registration successful! You can now log in.")
        else:
            st.error(f"Failed to register. Status code: {reg_response.status_code}")
            st.write(reg_response.text)

elif auth_mode == "Login":
    # 用户登录
    st.header("Login to Your Account")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        response = requests.post(api_url_token, data={"username": username, "password": password})
        
        if response.status_code == 200:
            json_response = response.json()
            token = json_response["access_token"]
            st.session_state.token = token
            st.success("Logged in successfully!")
        else:
            st.error(f"Failed to log in. Status code: {response.status_code}")
            st.write(response.text)

# 显示 PDF 列表及处理功能
if "token" in st.session_state:
    st.title("Select PDF for Processing")
    
    headers = {"Authorization": f"Bearer {st.session_state.token}"}
    pdf_response = requests.get(api_url_main, headers=headers)
    
    if pdf_response.status_code == 200:
        pdf_list = pdf_response.json().get("pdfs", [])
        selected_pdf = st.selectbox("Select PDF", pdf_list)

        user_input = st.text_area("Enter your query")
        
        if st.button("Process PDF"):
            process_response = requests.post(
                api_url_process,
                data={"pdf_name": selected_pdf, "user_input": user_input},
                headers=headers
            )
            if process_response.status_code == 200:
                result = process_response.json()
                st.write(f"Processed Content: {result['processed_content']}")
                st.write(f"AI Response: {result['openai_response']}")
            else:
                st.error(f"Error processing PDF. Status code: {process_response.status_code}")
                st.write(process_response.text)
    else:
        st.error(f"Failed to fetch PDF list. Status code: {pdf_response.status_code}")
        st.write(pdf_response.text)