# airflow/Dockerfile
FROM python:3.10-slim

# Set working directory
WORKDIR /app/airflow

# Copy dependencies
COPY airflow/pyproject.toml airflow/poetry.lock /app/airflow/

# Install Poetry and dependencies
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Copy Airflow code
COPY airflow/ /app/airflow/

# Initialize Airflow database
RUN poetry run airflow db init

# Expose port 8080 for Airflow web server
EXPOSE 8080

# Run Airflow web server
CMD ["poetry", "run", "airflow", "webserver"]