"""
Oracle connectivity test (thin mode only; no SYSDBA/SYSOPER role).

It prints either:
- SUCCESS: Oracle connection successful
or
- ERROR: <details>

Credentials are read from environment variables so secrets are not committed to git.
"""

import os
import re

import oracledb


def main() -> None:
    hostname = os.environ.get("ORACLE_HOST", "").strip()
    port_raw = os.environ.get("ORACLE_PORT", "1521").strip()
    service_name = os.environ.get("ORACLE_SERVICE_NAME", "").strip()
    username = os.environ.get("ORACLE_USERNAME", "").strip()
    password = os.environ.get("ORACLE_PASSWORD", "").strip()
    schema_name = os.environ.get("ORACLE_SCHEMA", "").strip()

    if not hostname or not service_name or not username or not password:
        print("ERROR: Set ORACLE_HOST, ORACLE_SERVICE_NAME, ORACLE_USERNAME, ORACLE_PASSWORD")
        return

    try:
        port = int(port_raw) if port_raw else 1521
    except ValueError:
        port = 1521

    thin = oracledb.is_thin_mode()
    print(f"THIN_MODE: {thin}")
    if not thin:
        print(
            "ERROR: Driver is in thick mode. This test requires thin mode only "
            "(do not call init_oracle_client())."
        )
        return

    dsn = oracledb.makedsn(hostname, port, service_name=service_name)

    try:
        conn = oracledb.connect(user=username, password=password, dsn=dsn)
        try:
            with conn.cursor() as cursor:
                if schema_name:
                    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*", schema_name):
                        raise ValueError(f"Invalid schema_name: {schema_name!r}")
                    cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema_name.upper()}")
                cursor.execute("SELECT 1 FROM DUAL")
                _ = cursor.fetchone()
            print("SUCCESS: Oracle connection successful")
        finally:
            conn.close()
    except oracledb.Error as e:
        code = None
        msg = str(e)
        try:
            err = e.args[0]
            code = getattr(err, "code", None)
            msg = getattr(err, "message", None) or msg
        except Exception:
            pass

        if code is not None:
            print(f"ERROR: ORA-{int(code):05d}: {msg}".rstrip())
        else:
            print(f"ERROR: {msg}".rstrip())
    except Exception as e:
        print(f"ERROR: {e}".rstrip())


if __name__ == "__main__":
    main()
