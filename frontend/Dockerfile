FROM python:3.10-slim

WORKDIR /app/frontend

# 複製依賴文件
COPY frontend/pyproject.toml frontend/poetry.lock /app/frontend/

# 安裝 Poetry 及依賴
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

# 複製前端代碼
COPY frontend/ /app/frontend/

EXPOSE 8501
CMD ["poetry", "run", "streamlit", "run", "streamlit_test.py", "--server.port=8501", "--server.address=0.0.0.0"]
