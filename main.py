import sys
from src.config import PipelineConfig
from src.extract import extract_from_source, extract_siafem_data, extract_sigeo_data, extract_combined_financial_data
from src.transform import clean_financial_data, calculate_execution_rate, calculate_monthly_variance, calculate_annual_variance
from src.load import load_with_retry
from src.logger import setup_logger

logger = setup_logger(__name__)

def run_pipeline():
    logger.info("="*60)
    logger.info("Starting ETL Pipeline Execution")
    logger.info("="*60)

    try:
        config = PipelineConfig.from_env()
        logger.info(f"Configuration loaded: Project={config.project_id}, Table={config.table_id}")

        logger.info("PHASE 1: EXTRACT")
        # Try to extract from APIs first, fallback to CSV file
        if config.siafem_api_url or config.sigeo_api_url:
            raw_data = extract_combined_financial_data(
                siafem_endpoint=config.siafem_api_url,
                sigeo_endpoint=config.sigeo_api_url
            )
        else:
            raw_data = extract_from_source(config.source_file)

        logger.info("PHASE 2: TRANSFORM")
        transformed_data = clean_financial_data(raw_data)
        transformed_data = calculate_execution_rate(transformed_data)
        transformed_data = calculate_monthly_variance(transformed_data)
        transformed_data = calculate_annual_variance(transformed_data)

        logger.info("PHASE 3: LOAD")
        load_with_retry(transformed_data, config)

        logger.info("="*60)
        logger.info("ETL Pipeline completed successfully")
        logger.info("="*60)
        return 0

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        logger.info("="*60)
        return 1

if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)