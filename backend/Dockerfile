# backend/Dockerfile
FROM python:3.10-slim

WORKDIR /app/backend

# 安装必要的 ODBC 驱动和 pyodbc
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    apt-transport-https \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    libgss3 \
    libsasl2-modules \
    odbcinst \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 安装 pyodbc 以代替 msodbcsql17
RUN pip install pymssql

# 复制依赖文件
COPY backend/pyproject.toml backend/poetry.lock /app/backend/

# 安装 Poetry 及依赖
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

# 复制后端代码
COPY backend/ /app/backend/

# 设置 PYTHONPATH
ENV PYTHONPATH=/app

# 暴露后端服务的端口
EXPOSE 8000

# 启动后端服务
CMD ["poetry", "run", "uvicorn", "backend.fastapi_code:app", "--host", "0.0.0.0", "--port", "8000"]