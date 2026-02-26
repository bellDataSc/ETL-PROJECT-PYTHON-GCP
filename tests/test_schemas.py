import unittest
from datetime import datetime, date
from src.schemas import FinancialMetricsSchema

class TestSchemas(unittest.TestCase):

    def test_validate_complete_row(self):
        row = {
            "data_coleta": datetime.now(),
            "fonte_dados": "SIAFEM",
            "municipio_codigo": "3106200",
            "municipio_nome": "SÃO PAULO",
            "orcamento_previsto": 1000000.0,
            "orcamento_executado": 850000.0,
            "taxa_execucao": 85.0,
            "despesa_por_funcao": "EDUCACAO",
            "valor_despesa": 500000.0,
            "programa_codigo": "001",
            "programa_nome": "PROGRAMA_PRINCIPAL",
            "rubrica_orcamentaria": "3190.39",
            "periodo_referencia": date.today(),
            "variacao_mensal": 2.5,
            "variacao_anual": 5.0,
            "validacao_status": "VALID",
            "processed_timestamp": datetime.now()
        }
        self.assertTrue(FinancialMetricsSchema.validate_row(row))

    def test_validate_incomplete_row(self):
        row = {"fonte_dados": "SIAFEM", "municipio_codigo": "3106200"}
        self.assertFalse(FinancialMetricsSchema.validate_row(row))

    def test_bigquery_schema_generation(self):
        schema = FinancialMetricsSchema.get_bigquery_schema()
        self.assertEqual(len(schema), len(FinancialMetricsSchema.FIELDS))
        self.assertTrue(all(s["mode"] == "NULLABLE" for s in schema))

if __name__ == "__main__":
    unittest.main()