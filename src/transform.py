import pandas as pd
from src.logger import setup_logger
from src.schemas import FinancialMetricsSchema
from typing import Dict, List
from datetime import datetime
import numpy as np

logger = setup_logger(__name__)

def clean_financial_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize SIAFEM and SIGEO financial data.
    Handles numeric conversion, missing values and text standardization.
    """
    logger.info(f"Starting transformation on {len(df)} rows")

    df_cleaned = df.copy()

    # Rename common column variations
    column_mapping = {
        "valor": "valor_despesa",
        "orcamento": "orcamento_previsto",
        "executado": "orcamento_executado",
        "funcao": "despesa_por_funcao",
        "municipio": "municipio_nome"
    }

    for old_col, new_col in column_mapping.items():
        if old_col in df_cleaned.columns and new_col not in df_cleaned.columns:
            df_cleaned.rename(columns={old_col: new_col}, inplace=True)

    # Convert numeric columns
    numeric_columns = [
        "orcamento_previsto", "orcamento_executado", "valor_despesa",
        "variacao_mensal", "variacao_anual", "taxa_execucao"
    ]

    for col in numeric_columns:
        if col in df_cleaned.columns:
            initial_nulls = df_cleaned[col].isna().sum()
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")
            new_nulls = df_cleaned[col].isna().sum()
            if new_nulls > initial_nulls:
                logger.warning(f"Converted {new_nulls - initial_nulls} invalid values to NULL in {col}")

    # Drop rows with critical missing values
    df_cleaned = df_cleaned.dropna(subset=["orcamento_previsto", "periodo_referencia"])
    logger.info(f"Removed rows with missing critical fields")

    # Standardize text fields
    text_columns = [
        "municipio_nome", "despesa_por_funcao", "programa_nome",
        "rubrica_orcamentaria", "fonte_dados"
    ]

    for col in text_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].str.strip().str.upper()

    # Convert date columns
    if "periodo_referencia" in df_cleaned.columns:
        df_cleaned["periodo_referencia"] = pd.to_datetime(
            df_cleaned["periodo_referencia"], errors="coerce"
        ).dt.date

    # Add processing metadata
    if "data_coleta" not in df_cleaned.columns:
        df_cleaned["data_coleta"] = datetime.utcnow()

    if "validacao_status" not in df_cleaned.columns:
        df_cleaned["validacao_status"] = "VALID"

    df_cleaned["processed_timestamp"] = datetime.utcnow()

    valid_rows = len(df_cleaned)
    logger.info(f"Transformation complete: {valid_rows} rows processed")

    return df_cleaned

def calculate_execution_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate budget execution rate (taxa_execucao) from SIAFEM data.
    taxa_execucao = (orcamento_executado / orcamento_previsto) * 100
    """
    if "orcamento_previsto" not in df.columns or "orcamento_executado" not in df.columns:
        logger.warning("Required columns for execution rate calculation not found")
        return df

    df_calc = df.copy()

    # Avoid division by zero
    mask = df_calc["orcamento_previsto"] != 0
    df_calc.loc[mask, "taxa_execucao"] = (
        (df_calc.loc[mask, "orcamento_executado"] / df_calc.loc[mask, "orcamento_previsto"]) * 100
    )
    df_calc.loc[~mask, "taxa_execucao"] = 0.0

    logger.info("Calculated budget execution rates")
    return df_calc

def calculate_monthly_variance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate month-over-month variance for budget and spending metrics.
    """
    if "valor_despesa" not in df.columns or "periodo_referencia" not in df.columns:
        logger.warning("Required columns for variance calculation not found")
        return df

    df_sorted = df.sort_values("periodo_referencia", ascending=True)
    df_sorted["variacao_mensal"] = df_sorted.groupby(
        ["municipio_codigo", "programa_codigo"]
    )["valor_despesa"].pct_change() * 100

    logger.info("Calculated monthly variances")
    return df_sorted

def calculate_annual_variance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate year-over-year variance for budget and spending metrics.
    """
    if "valor_despesa" not in df.columns or "periodo_referencia" not in df.columns:
        logger.warning("Required columns for annual variance calculation not found")
        return df

    df_sorted = df.sort_values("periodo_referencia", ascending=True)
    df_sorted["data_coleta_year_shifted"] = df_sorted.groupby(
        ["municipio_codigo", "programa_codigo"]
    )["periodo_referencia"].shift(12)

    df_sorted["variacao_anual"] = df_sorted.groupby(
        ["municipio_codigo", "programa_codigo"]
    )["valor_despesa"].pct_change() * 100

    df_sorted = df_sorted.drop(columns=["data_coleta_year_shifted"])
    logger.info("Calculated annual variances")
    return df_sorted