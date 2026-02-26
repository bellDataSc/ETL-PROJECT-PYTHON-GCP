# ETL-PROJECT-PYTHON-GCP

This project contains a production-ready ETL pipeline built in Python and designed to run on Google Cloud Platform using BigQuery as the main storage layer. The pipeline focuses on Brazilian public financial data from SIAFEM and SIGEO systems, supporting ingestion from CSV files or HTTP APIs, transformation of raw values into standardized metrics and loading into curated tables for analytics.

The main goal is to provide a clean and reproducible data engineering flow that can be reused as a template for similar financial data projects.

## What this project does

- Reads raw financial data from SIAFEM and SIGEO systems via APIs or local files
- Cleans and standardizes column names and numeric types
- Validates budget execution, expenditure hierarchies and financial dimensions
- Calculates helper metrics such as budget utilization rates and monthly variance
- Validates required fields before loading
- Loads the final dataset into a BigQuery table

## How to run

1. Create a service account in GCP with BigQuery permissions and download the JSON key.
2. Set the environment variables:

   - `GCP_PROJECT_ID`
   - `GCP_DATASET_ID`
   - `GCP_TABLE_ID`
   - `SOURCE_FILE` (path to the CSV file, if used)
   - `SIAFEM_API_URL` (optional: SIAFEM API endpoint)
   - `SIGEO_API_URL` (optional: SIGEO API endpoint)

3. Install the dependencies and run the pipeline:

   ```bash
   pip install -r requirements.txt
   python main.py
   ```

4. Or run everything with Docker:

   ```bash
   docker build -t etl-project-python-gcp .
   docker run \
     -e GCP_PROJECT_ID=your-project \
     -e GCP_DATASET_ID=financial_metrics \
     -e GCP_TABLE_ID=budget_execution \
     -e SOURCE_FILE=/app/data/input.csv \
     -e SIAFEM_API_URL=https://api.siafem.gov.br/dados \
     -e SIGEO_API_URL=https://api.sigeo.gov.br/dados \
     -v /path/to/credentials.json:/app/credentials.json \
     -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
     etl-project-python-gcp
   ```

## Why this repository exists

I created this pipeline to automate the ingestion and standardization of economic indicators from various Brazilian government agencies and financial institutions while working for the Government of São Paulo.

This is the third version. I've incorporated improvements using Open Source technologies and what I was using at the time. I like to keep everything documented, like a personal data engineering library.

The project was used to reduce manual reconciliation of budget execution metrics and to feed dashboards and forecasting models with consistent financial series based on real work in data engineering.

## Quick Start with Google Colab

You can run this pipeline interactively using Google Colab without setting up your local environment:

1. Open [`notebooks/ETL_Pipeline_Colab.ipynb`](notebooks/ETL_Pipeline_Colab.ipynb) in Google Colab
2. Follow the notebook cells to authenticate with GCP and run the pipeline
3. Visualize and analyze the results directly in Colab

This is perfect for data exploration, testing, and prototyping before running in production.

Made with ☕ by Isabel Cruz