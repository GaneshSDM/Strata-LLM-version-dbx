from .postgresql import PostgreSQLAdapter
from .mysql import MySQLAdapter
from .snowflake import SnowflakeAdapter
from .databricks import DatabricksAdapter
from .sqlserver import SQLServerAdapter
from .teradata import TeradataAdapter
from .bigquery import BigQueryAdapter
from .s3 import S3Adapter

try:
    from .oracle import OracleAdapter
except ModuleNotFoundError:
    OracleAdapter = None

ADAPTERS = {
    "PostgreSQL": PostgreSQLAdapter,
    "MySQL": MySQLAdapter,
    "Snowflake": SnowflakeAdapter,
    "Databricks": DatabricksAdapter,
    **({"Oracle": OracleAdapter} if OracleAdapter else {}),
    "SQL Server": SQLServerAdapter,
    "Teradata": TeradataAdapter,
    "Google BigQuery": BigQueryAdapter,
    "AWS S3": S3Adapter
}

def get_adapter(db_type: str, credentials: dict):
    adapter_class = ADAPTERS.get(db_type)
    if not adapter_class:
        if db_type == "Oracle":
            raise ValueError("Oracle adapter unavailable. Install cx_Oracle or oracledb.")
        raise ValueError(f"Unsupported database type: {db_type}")
    return adapter_class(credentials)
