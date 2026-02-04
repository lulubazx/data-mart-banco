import pandas as pd
import pytest

from src.ingestion.extractors import build_db_connection
from src.ingestion.transformers import convert_date_columns


def test_db_connection_missing_env(monkeypatch):
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    with pytest.raises(RuntimeError):
        build_db_connection()


def test_date_conversion():
    df = pd.DataFrame({"created_at": ["2023-01-01", "2023-02-01"]})
    cfg = {"date_columns": ["created_at"]}
    result = convert_date_columns(df, cfg, logger=None)
    assert str(result["created_at"].dtype) == "datetime64[ns, UTC]"
