import os

import yaml


CONFIG_TABLES_PATH = os.getenv("INGESTION_CONFIG_PATH", "config/tables.yaml")
CONFIG_SCHEMAS_PATH = os.getenv("INGESTION_SCHEMAS_PATH", "config/schemas.yaml")


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_tables_config():
    return load_yaml(CONFIG_TABLES_PATH)


def load_schemas_config():
    return load_yaml(CONFIG_SCHEMAS_PATH)
