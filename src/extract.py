import pandas as pd
from src.logger import setup_logger
from typing import Optional
import requests
from datetime import datetime

logger = setup_logger(__name__)

def extract_from_source(file_path: str) -> pd.DataFrame:
    logger.info(f"Extracting data from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV: {str(e)}")
        raise

def extract_siafem_data(api_endpoint: Optional[str] = None, year: int = None, month: int = None) -> pd.DataFrame:
    """
    Extract budget execution data from SIAFEM (Sistema de Informações sobre Orçamentos Públicos).
    Retrieves government spending data, budget utilization and expenditure by function/program.
    """
    if not api_endpoint:
        logger.warning("No SIAFEM API endpoint provided, returning empty DataFrame")
        return pd.DataFrame()

    logger.info(f"Fetching SIAFEM budget data from {api_endpoint}")
    try:
        params = {}
        if year:
            params["ano"] = year
        if month:
            params["mes"] = month

        response = requests.get(api_endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        logger.info(f"Successfully fetched {len(df)} SIAFEM records")
        return df
    except requests.RequestException as e:
        logger.error(f"SIAFEM API request failed: {str(e)}")
        raise

def extract_sigeo_data(api_endpoint: Optional[str] = None, municipio: Optional[str] = None) -> pd.DataFrame:
    """
    Extract budget planning data from SIGEO (Sistema de Informação e Gestão Orçamentária).
    Retrieves municipal budget information, planning goals and budget allocations.
    """
    if not api_endpoint:
        logger.warning("No SIGEO API endpoint provided, returning empty DataFrame")
        return pd.DataFrame()

    logger.info(f"Fetching SIGEO budget data from {api_endpoint}")
    try:
        params = {}
        if municipio:
            params["municipio"] = municipio

        response = requests.get(api_endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        logger.info(f"Successfully fetched {len(df)} SIGEO records")
        return df
    except requests.RequestException as e:
        logger.error(f"SIGEO API request failed: {str(e)}")
        raise

def extract_combined_financial_data(
    siafem_endpoint: Optional[str] = None,
    sigeo_endpoint: Optional[str] = None,
    year: int = None,
    month: int = None,
    municipio: Optional[str] = None
) -> pd.DataFrame:
    """
    Combines SIAFEM and SIGEO data for comprehensive financial analysis.
    Returns a unified DataFrame with data from both sources.
    """
    logger.info("Extracting combined SIAFEM and SIGEO financial data")
    dfs = []

    if siafem_endpoint:
        siafem_df = extract_siafem_data(siafem_endpoint, year, month)
        if not siafem_df.empty:
            siafem_df["fonte_dados"] = "SIAFEM"
            dfs.append(siafem_df)

    if sigeo_endpoint:
        sigeo_df = extract_sigeo_data(sigeo_endpoint, municipio)
        if not sigeo_df.empty:
            sigeo_df["fonte_dados"] = "SIGEO"
            dfs.append(sigeo_df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} records from SIAFEM and SIGEO")
        return combined_df
    else:
        logger.warning("No data retrieved from either SIAFEM or SIGEO")
        return pd.DataFrame()