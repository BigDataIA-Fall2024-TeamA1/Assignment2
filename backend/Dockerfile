FROM python:3.10-slim

WORKDIR /app/backend

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        apt-transport-https \
        gnupg2 \
        unixodbc \
        unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY backend/pyproject.toml backend/poetry.lock /app/backend/

# 安装 Poetry 及依赖
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

# 复制后端代码
COPY backend/ /app/backend/

# 設置 PYTHONPATH
ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "backend.fastapi_code:app", "--host", "0.0.0.0", "--port", "8000"]
