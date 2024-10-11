import streamlit as st
import requests
import sys
import os
import base64
from io import BytesIO

# Add the root directory of the project to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# API URLs
api_url_register = "http://backend:8000/register"
api_url_token = "http://backend:8000/token"
api_url_main = "http://backend:8000/main"
api_url_preview = "http://backend:8000/preview-pdf"
api_url_process_pdf = "http://backend:8000/process-pdf"

# Streamlit App Configuration
st.set_page_config(page_title="PDF Processing Tool", layout="wide", initial_sidebar_state="expanded")

# Apply custom CSS for better UI
def apply_custom_css():
    st.markdown(
        """
        <style>
        body {
            background-color: #f0f2f6;
            color: #333;
        }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            border: none;
            padding: 10px 20px;
            text-align: center;
            font-size: 16px;
            margin: 10px 0;
            cursor: pointer;
        }
        .stButton>button:hover {
            background-color: #45a049;
        }
        .stRadio>div>label {
            font-weight: bold;
        }
        .stTextInput>div>label {
            font-weight: bold;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

apply_custom_css()

# Handle Logout
def logout():
    st.session_state.clear()
    st.session_state.page = "Login"

# Login Page
def login_page():
    st.title("Login to Your Account")
    username = st.text_input("Username", key="login_username")
    password = st.text_input("Password", type="password", key="login_password")
    if st.button("Login"):
        response = requests.post(api_url_token, data={"username": username, "password": password})
        if response.status_code == 200:
            json_response = response.json()
            st.session_state.token = json_response["access_token"]
            st.session_state.logged_in = True
            st.session_state.page = "PDF Selection"
        else:
            st.error(f"Failed to log in. Status code: {response.status_code}")
            st.write(response.text)
    if st.button("Go to Register"):
        st.session_state.page = "Register"

# Register Page
def register_page():
    st.title("Register a New Account")
    reg_username = st.text_input("New Username", key="register_username")
    reg_password = st.text_input("New Password", type="password", key="register_password")
    if st.button("Register"):
        reg_response = requests.post(api_url_register, data={"username": reg_username, "password": reg_password})
        if reg_response.status_code == 200:
            st.success("Registration successful! Please log in.")
            st.session_state.page = "Login"
        elif reg_response.status_code == 400:
            st.error("Username already exists.")
        else:
            st.error(f"Failed to register. Status code: {reg_response.status_code}")
            st.write(reg_response.json().get("error", "Unknown error"))

# PDF Selection Page
def pdf_selection_page():
    st.title("Select PDF for Processing")
    headers = {"Authorization": f"Bearer {st.session_state.token}"}
    pdf_response = requests.get(api_url_main, headers=headers)

    if pdf_response.status_code == 200:
        pdf_list = pdf_response.json().get("pdfs", [])
        selected_pdf = st.selectbox("Select PDF", pdf_list)

        if selected_pdf:
            # Preview PDF section
            if 'pdf_preview' not in st.session_state or st.session_state.selected_pdf != selected_pdf:
                # Request preview content for selected PDF
                pdf_blob_response = requests.post(
                    api_url_preview,
                    data={"pdf_name": selected_pdf},
                    headers=headers
                )
                if pdf_blob_response.status_code == 200:
                    pdf_blob = pdf_blob_response.content
                    base64_pdf = base64.b64encode(pdf_blob).decode('utf-8')
                    st.session_state.pdf_preview = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="600px"></iframe>'
                    st.session_state.selected_pdf = selected_pdf
                    st.session_state.download_data = pdf_blob
                else:
                    st.error(f"Error fetching preview. Status code: {pdf_blob_response.status_code}")

            # Display the preview if available
            if 'pdf_preview' in st.session_state:
                st.markdown(st.session_state.pdf_preview, unsafe_allow_html=True)
                st.download_button(label="Download PDF", data=st.session_state.download_data, file_name=selected_pdf, mime="application/pdf")

            # Extraction section
            extract_option = st.radio("Choose extraction method", ["Azure AI Document Intelligence", "PyMuDF"], index=0)
            user_input = st.text_area("Enter your query related to the selected PDF")

            if st.button("Fetch Extract and Get Answer"):
                # Determine the correct extract filename and API URL based on the extraction method
                process_response = requests.post(
                    api_url_process_pdf,
                    data={
                        "pdf_name": selected_pdf,
                        "extract_method": extract_option,
                        "user_input": user_input
                    },
                    headers=headers
                )
                if process_response.status_code == 200:
                    result = process_response.json()
                    st.write(f"Processed Content: {result['processed_content']}")
                    if 'openai_response' in result:
                        st.write(f"AI Response: {result['openai_response']}")
                else:
                    st.error(f"Error processing PDF. Status code: {process_response.status_code}")
                    st.write(process_response.text)
    else:
        st.error(f"Failed to fetch PDF list. Status code: {pdf_response.status_code}")
        st.write(pdf_response.text)


# Main Logic
if 'page' not in st.session_state:
    st.session_state.page = "Login"

if st.session_state.page == "Login":
    login_page()
elif st.session_state.page == "Register":
    register_page()
elif st.session_state.page == "PDF Selection":
    if 'logged_in' in st.session_state and st.session_state.logged_in:
        pdf_selection_page()
    else:
        st.session_state.page = "Login"

# Sidebar Logout Option
if 'logged_in' in st.session_state and st.session_state.logged_in:
    st.sidebar.button("Logout", on_click=logout)
