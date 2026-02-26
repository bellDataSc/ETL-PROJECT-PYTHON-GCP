import unittest
import pandas as pd
from datetime import datetime, date
from src.transform import clean_financial_data, calculate_execution_rate, calculate_monthly_variance
from src.schemas import FinancialMetricsSchema

class TestTransform(unittest.TestCase):

    def setUp(self):
        self.sample_data = pd.DataFrame({
            "municipio_codigo": ["3106200", "3106200", "3504008"],
            "municipio_nome": ["são paulo", "são paulo", "rio de janeiro"],
            "fonte_dados": ["siafem", "siafem", "sigeo"],
            "orcamento_previsto": [1000000.0, 1500000.0, 2000000.0],
            "orcamento_executado": [850000.0, 1200000.0, 1800000.0],
            "valor_despesa": [500000.0, 600000.0, 750000.0],
            "programa_codigo": ["001", "002", "001"],
            "programa_nome": ["educacao", "saude", "educacao"],
            "periodo_referencia": ["2024-01-31", "2024-02-29", "2024-01-31"]
        })

    def test_clean_financial_data(self):
        result = clean_financial_data(self.sample_data)
        self.assertEqual(len(result), 3)
        self.assertIn("validacao_status", result.columns)
        self.assertIn("processed_timestamp", result.columns)
        self.assertTrue(all(result["municipio_nome"].str.isupper()))
        self.assertTrue(all(result["fonte_dados"].str.isupper()))

    def test_remove_invalid_rows(self):
        invalid_data = self.sample_data.copy()
        invalid_data.loc[0, "orcamento_previsto"] = None
        result = clean_financial_data(invalid_data)
        self.assertEqual(len(result), 2)

    def test_execution_rate_calculation(self):
        result = calculate_execution_rate(self.sample_data)
        self.assertIn("taxa_execucao", result.columns)
        # First row: 850000 / 1000000 * 100 = 85%
        self.assertAlmostEqual(result.iloc[0]["taxa_execucao"], 85.0, places=2)

    def test_monthly_variance(self):
        result = calculate_monthly_variance(self.sample_data)
        self.assertIn("variacao_mensal", result.columns)

if __name__ == "__main__":
    unittest.main()