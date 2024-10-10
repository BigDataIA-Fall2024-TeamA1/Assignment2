# airflow/Dockerfile
FROM python:3.10-slim

# 設置工作目錄
WORKDIR /app/airflow

# 複製依賴文件
COPY airflow/pyproject.toml airflow/poetry.lock /app/airflow/

# 安裝 Poetry 及依賴
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# 複製 Airflow 代碼
COPY airflow/ /app/airflow/

# 初始化 Airflow 數據庫
RUN poetry run airflow db init

# 暴露端口
EXPOSE 8080

# 啟動命令
CMD ["poetry", "run", "airflow", "webserver"]
