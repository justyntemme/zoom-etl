# Base Stage
#FROM python:3.8-slim AS base
FROM python:3.12-slim AS base

ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem .
ADD query_data .
ADD src .
ADD main.py helpers.py requirements.txt .env ./

RUN pip install --no-cache-dir -r requirements.txt

# Development Stage
FROM base AS dev
COPY . .
CMD ["python3", "main.py", "./query_data"]

# Production Stage
FROM base AS prod
COPY . . 
CMD ["python3", "src/fetch_secrets.py", "&&", "python3", "main.py", "./query_data"]