from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd
from src.logger import setup_logger
from src.config import PipelineConfig
from src.schemas import FinancialMetricsSchema
import time

logger = setup_logger(__name__)

def load_to_bigquery(
    df: pd.DataFrame,
    config: PipelineConfig,
    job_config_overrides: dict = None
) -> None:
    logger.info(f"Loading {len(df)} rows to BigQuery table {config.dataset_id}.{config.table_id}")

    try:
        client = bigquery.Client(project=config.project_id)
        table_id = f"{config.project_id}.{config.dataset_id}.{config.table_id}"

        schema = [
            bigquery.SchemaField(field, dtype.value, mode="NULLABLE")
            for field, dtype in FinancialMetricsSchema.FIELDS.items()
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=config.write_disposition,
            autodetect=False
        )

        if job_config_overrides:
            for key, value in job_config_overrides.items():
                setattr(job_config, key, value)

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result(timeout=config.timeout_seconds)
        logger.info(f"Successfully loaded data to {table_id}")
        logger.info(f"Rows loaded: {job.output_rows}")

    except GoogleCloudError as e:
        logger.error(f"BigQuery error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during load: {str(e)}")
        raise

def load_with_retry(
    df: pd.DataFrame,
    config: PipelineConfig,
    max_retries: int = None
) -> None:
    max_retries = max_retries or config.max_retries
    attempt = 1

    while attempt <= max_retries:
        try:
            load_to_bigquery(df, config)
            return
        except Exception as e:
            logger.warning(f"Load attempt {attempt} failed: {str(e)}")
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1
            else:
                logger.error(f"Failed after {max_retries} attempts")
                raise