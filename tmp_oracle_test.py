"""
Oracle connectivity test via Strata's OracleAdapter.

To run (PowerShell):
  $env:ORACLE_HOST='...'
  $env:ORACLE_PORT='1521'
  $env:ORACLE_SERVICE_NAME='...'
  $env:ORACLE_USERNAME='...'
  $env:ORACLE_PASSWORD='...'
  $env:ORACLE_SCHEMA='SUPER_ADMIN'   # optional
  $env:ORACLE_ROLE='SYSDBA'          # optional (requires thick mode)
  $env:ORACLE_CLIENT_LIB_DIR='C:\\path\\to\\instantclient'  # optional
  python .\\tmp_oracle_test.py
"""

import asyncio
import os
import sys

# Ensure backend modules can be imported when running from repo root
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from backend.adapters.oracle import OracleAdapter


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


async def main():
    host = _env("ORACLE_HOST")
    service_name = _env("ORACLE_SERVICE_NAME")
    username = _env("ORACLE_USERNAME")
    password = _env("ORACLE_PASSWORD")

    if not host or not service_name or not username or not password:
        print("ERROR: Set ORACLE_HOST, ORACLE_SERVICE_NAME, ORACLE_USERNAME, ORACLE_PASSWORD")
        return

    port_raw = _env("ORACLE_PORT", "1521")
    try:
        port = int(port_raw) if port_raw else 1521
    except ValueError:
        port = 1521

    credentials = {
        "host": host,
        "port": port,
        "service_name": service_name,
        "username": username,
        "password": password,
    }

    schema = _env("ORACLE_SCHEMA")
    if schema:
        credentials["schema"] = schema

    role = _env("ORACLE_ROLE")
    if role:
        credentials["role"] = role

    lib_dir = _env("ORACLE_CLIENT_LIB_DIR")
    if lib_dir:
        credentials["oracle_client_lib_dir"] = lib_dir

    adapter = OracleAdapter(credentials)
    result = await adapter.test_connection()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
