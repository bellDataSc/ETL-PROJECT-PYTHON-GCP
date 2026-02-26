from typing import Dict, List
from enum import Enum

class DataType(Enum):
    STRING = "STRING"
    FLOAT64 = "FLOAT64"
    INT64 = "INT64"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"

class FinancialMetricsSchema:
    """
    Schema for SIAFEM and SIGEO financial data.
    Supports budget execution tracking, expenditure analysis and municipal financial planning.
    """
    FIELDS = {
        "data_coleta": DataType.TIMESTAMP,
        "fonte_dados": DataType.STRING,  # SIAFEM or SIGEO
        "municipio_codigo": DataType.STRING,
        "municipio_nome": DataType.STRING,
        "orcamento_previsto": DataType.FLOAT64,
        "orcamento_executado": DataType.FLOAT64,
        "taxa_execucao": DataType.FLOAT64,
        "despesa_por_funcao": DataType.STRING,
        "valor_despesa": DataType.FLOAT64,
        "programa_codigo": DataType.STRING,
        "programa_nome": DataType.STRING,
        "rubrica_orcamentaria": DataType.STRING,
        "periodo_referencia": DataType.DATE,
        "variacao_mensal": DataType.FLOAT64,
        "variacao_anual": DataType.FLOAT64,
        "validacao_status": DataType.STRING,
        "processed_timestamp": DataType.TIMESTAMP
    }

    @classmethod
    def validate_row(cls, row: Dict) -> bool:
        required_fields = {"fonte_dados", "municipio_codigo", "orcamento_previsto", "periodo_referencia"}
        return all(field in row for field in required_fields)

    @classmethod
    def get_bigquery_schema(cls) -> List[Dict]:
        return [
            {"name": field, "type": dtype.value, "mode": "NULLABLE"}
            for field, dtype in cls.FIELDS.items()
        ]