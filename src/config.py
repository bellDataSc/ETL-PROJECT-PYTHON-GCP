from dataclasses import dataclass
from typing import Optional
import os

@dataclass
class PipelineConfig:
    project_id: str
    dataset_id: str
    table_id: str
    source_file: str
    siafem_api_url: Optional[str] = None
    sigeo_api_url: Optional[str] = None
    log_level: str = "INFO"
    write_disposition: str = "WRITE_TRUNCATE"
    max_retries: int = 3
    timeout_seconds: int = 300

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID"),
            dataset_id=os.getenv("GCP_DATASET_ID"),
            table_id=os.getenv("GCP_TABLE_ID"),
            source_file=os.getenv("SOURCE_FILE", "data/input.csv"),
            siafem_api_url=os.getenv("SIAFEM_API_URL"),
            sigeo_api_url=os.getenv("SIGEO_API_URL"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            write_disposition=os.getenv("WRITE_DISPOSITION", "WRITE_TRUNCATE"),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            timeout_seconds=int(os.getenv("TIMEOUT_SECONDS", "300"))
        )