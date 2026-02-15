from backend.main import _sanitize_credentials


def test_sanitize_databricks_credentials_aliases():
    raw = {
        "host": "dbc.example.com",
        "httpPath": "/sql/1.0/endpoints/x",
        "accessToken": "token123",
        "catalog": "main",
        "schema": "default",
    }
    cleaned = _sanitize_credentials("Databricks", raw)
    assert cleaned["server_hostname"] == "dbc.example.com"
    assert cleaned["http_path"] == "/sql/1.0/endpoints/x"
    assert cleaned["access_token"] == "token123"
    assert cleaned["catalog"] == "main"
    assert cleaned["schema"] == "default"


def test_sanitize_snowflake_credentials_aliases():
    raw = {
        "account": "acct",
        "user": "alice",
        "password": "secret",
        "warehouse": "wh",
        "db": "DB1",
        "schema": "PUBLIC",
    }
    cleaned = _sanitize_credentials("Snowflake", raw)
    assert cleaned["username"] == "alice"
    assert cleaned["database"] == "DB1"
    assert cleaned["schema"] == "PUBLIC"


def test_sanitize_mysql_credentials_aliases():
    raw = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "pw",
        "db": "app",
        "ssl": True,
    }
    cleaned = _sanitize_credentials("MySQL", raw)
    assert cleaned["username"] == "root"
    assert cleaned["database"] == "app"
    assert cleaned["ssl"] is True


def test_sanitize_oracle_whitelist():
    raw = {
        "host": "db.local",
        "port": 1521,
        "serviceName": "XE",
        "username": "sys",
        "password": "pw",
        "schema_name": "HR",
        "mode": "SYSDBA",
    }
    cleaned = _sanitize_credentials("Oracle", raw)
    assert cleaned["service_name"] == "XE"
    assert cleaned["schema"] == "HR"
    assert "mode" not in cleaned
