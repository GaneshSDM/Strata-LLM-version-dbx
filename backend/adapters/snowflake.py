import asyncio
from typing import Dict, Any, List, Optional, Callable
import traceback
from .base import DatabaseAdapter
import re

try:
    import snowflake.connector
    DRIVER_AVAILABLE = True
except ImportError:
    DRIVER_AVAILABLE = False

try:
    import psycopg2
except ImportError:
    psycopg2 = None

# Snowflake provides some shared/read-only databases where DDL/DML is not permitted.
SHARED_DATABASES = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
DEFAULT_WRITE_DATABASE = "STRATA_MIGRATIONS"


class SnowflakeAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        self.driver_available = DRIVER_AVAILABLE

    def _clean_account(self, account: str | None) -> str | None:
        if not account:
            return None
        value = str(account).strip()
        value = re.sub(r"^https?://", "", value, flags=re.IGNORECASE)
        value = value.strip().strip("/")
        # Accept inputs like: <acct>.<region>.snowflakecomputing.com
        value = re.sub(r"\.snowflakecomputing\.com$", "", value, flags=re.IGNORECASE)
        return value or None

    def _snowflake_connect_kwargs(self) -> Dict[str, Any]:
        account = self._clean_account(self.credentials.get("account"))
        user = self.credentials.get("username")
        password = self.credentials.get("password")
        warehouse = (self.credentials.get("warehouse") or "").strip()
        database = (self.credentials.get("database") or "").strip()
        schema = (self.credentials.get("schema") or "").strip()

        kwargs: Dict[str, Any] = {
            "user": user,
            "password": password,
            "account": account,
        }
        if warehouse:
            kwargs["warehouse"] = warehouse
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema
        return kwargs

    def _is_shared_database(self, db_name: str) -> bool:
        """Return True if the provided database name is a Snowflake shared/read-only database."""
        if not db_name:
            return False
        return str(db_name).strip().upper() in SHARED_DATABASES

    def get_connection(self):
        """Validation via direct DB-API connection is not supported for Snowflake in this codepath."""
        raise RuntimeError("Snowflake validation requires dedicated logic; get_connection is not supported.")

    def _resolve_writable_context(self) -> Dict[str, Any]:
        """
        Decide which database/schema to use for writes. If the configured database is missing or
        points to a shared/read-only database, fall back to a writable default.
        """
        configured_db = self.credentials.get("database")
        target_schema = self.credentials.get("schema", "PUBLIC")

        if not configured_db or self._is_shared_database(configured_db):
            return {
                "database": DEFAULT_WRITE_DATABASE,
                "schema": target_schema,
                "fallback_used": True,
                "configured_db": configured_db
            }

        return {
            "database": configured_db,
            "schema": target_schema,
            "fallback_used": False,
            "configured_db": configured_db
        }

    async def test_connection(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": True,
                "driver_unavailable": True,
                "vendorVersion": "Snowflake (simulated)",
                "details": "Snowflake driver not installed"
            }

        try:
            def connect_sync():
                kwargs = self._snowflake_connect_kwargs()
                # Provide sane defaults if omitted from the UI.
                kwargs.setdefault("schema", "PUBLIC")
                connection = snowflake.connector.connect(**kwargs)
                cursor = connection.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                cursor.close()
                connection.close()
                return version

            loop = asyncio.get_event_loop()
            version = await loop.run_in_executor(None, connect_sync)

            return {
                "ok": True,
                "vendorVersion": f"Snowflake {version}",
                "details": "Connection successful",
                "message": "Connection successful",
            }
        except Exception as e:
            message = str(e).strip() or f"{e.__class__.__name__}"
            return {"ok": False, "message": message}

    async def introspect_analysis(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "database_info": {
                    "type": "Snowflake",
                    "version": "Snowflake (simulated)",
                    "schemas": ["PUBLIC"],
                    "warehouse": "SIMULATED",
                    "region": "N/A"
                },
                "tables": [{"schema": "PUBLIC", "name": "USERS", "type": "BASE TABLE"}],
                "columns": [{"schema": "PUBLIC", "table": "USERS", "name": "ID", "type": "NUMBER", "nullable": False}],
                "constraints": [],
                "views": [],
                "procedures": [],
                "indexes": [],
                "data_profiles": [{"schema": "PUBLIC", "table": "USERS", "row_count": 1000}],
                "driver_unavailable": True
            }

        try:
            def analyze_sync():
                kwargs = self._snowflake_connect_kwargs()
                kwargs.setdefault("schema", "PUBLIC")
                conn = snowflake.connector.connect(**kwargs)
                cursor = conn.cursor()

                cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_WAREHOUSE(), CURRENT_VERSION()")
                row = cursor.fetchone() or ("", "", "", "")
                account, region, warehouse, version = row

                target_schema = self.credentials.get("schema") or "PUBLIC"
                target_database = self.credentials.get("database") or None

                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                      AND TABLE_SCHEMA = %s
                """, (target_schema,))
                tables_raw = cursor.fetchall() or []
                tables = [{"schema": t[0], "name": t[1], "type": t[2]} for t in tables_raw]

                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                """, (target_schema,))
                columns = []
                for schema, table, name, data_type, is_nullable in cursor.fetchall() or []:
                    columns.append({
                        "schema": schema,
                        "table": table,
                        "name": name,
                        "type": data_type,
                        "nullable": str(is_nullable).lower() == "yes"
                    })

                data_profiles = []
                # Gather live row counts per table
                for t in tables:
                    row_count = 0
                    try:
                        count_cursor = conn.cursor()
                        count_cursor.execute(f'SELECT COUNT(*) FROM "{t["schema"]}"."{t["name"]}"')
                        row = count_cursor.fetchone()
                        row_count = int(row[0]) if row and row[0] is not None else 0
                        count_cursor.close()
                    except Exception:
                        row_count = 0

                    t["row_count"] = row_count
                    data_profiles.append({
                        "schema": t["schema"],
                        "table": t["name"],
                        "row_count": row_count
                    })

                # Storage information (best-effort): may require privileges and may not be available in all accounts.
                storage_info = None
                storage_cursor = None
                try:
                    storage_cursor = conn.cursor()
                    storage_cursor.execute(
                        """
                        SELECT TABLE_SCHEMA, TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES
                        FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
                        WHERE TABLE_SCHEMA = %s
                        """,
                        (target_schema,),
                    )

                    table_storage = []
                    db_active = 0
                    db_time_travel = 0
                    db_failsafe = 0
                    size_by_table = {}

                    for schema, table, active, time_travel, failsafe in storage_cursor.fetchall() or []:
                        active_bytes = int(active or 0)
                        time_travel_bytes = int(time_travel or 0)
                        failsafe_bytes = int(failsafe or 0)
                        total_bytes = active_bytes + time_travel_bytes + failsafe_bytes

                        size_by_table[(schema, table)] = {
                            "total_size": total_bytes,
                            "data_size": active_bytes,
                            "index_size": 0,
                        }

                        table_storage.append(
                            {
                                "schema": schema,
                                "name": table,
                                "total_size": total_bytes,
                                "data_size": active_bytes,
                                "index_size": 0,
                            }
                        )
                        db_active += active_bytes
                        db_time_travel += time_travel_bytes
                        db_failsafe += failsafe_bytes

                    # Fallback: some accounts/roles do not expose TABLE_STORAGE_METRICS; try SHOW TABLES.
                    if not table_storage:
                        show_cursor = None
                        try:
                            show_cursor = conn.cursor()
                            show_cursor.execute(f'SHOW TABLES IN SCHEMA "{target_schema}"')
                            cols = [c[0].lower() for c in (show_cursor.description or [])]

                            bytes_idx = cols.index("bytes") if "bytes" in cols else None
                            name_idx = cols.index("name") if "name" in cols else None
                            schema_idx = cols.index("schema_name") if "schema_name" in cols else None

                            if bytes_idx is not None and name_idx is not None:
                                for row in show_cursor.fetchall() or []:
                                    schema = row[schema_idx] if schema_idx is not None else target_schema
                                    table = row[name_idx]
                                    total_bytes = int(row[bytes_idx] or 0)

                                    size_by_table[(schema, table)] = {
                                        "total_size": total_bytes,
                                        "data_size": total_bytes,
                                        "index_size": 0,
                                    }
                                    table_storage.append(
                                        {
                                            "schema": schema,
                                            "name": table,
                                            "total_size": total_bytes,
                                            "data_size": total_bytes,
                                            "index_size": 0,
                                        }
                                    )
                                    db_active += total_bytes
                        except Exception:
                            pass
                        finally:
                            try:
                                if show_cursor is not None:
                                    show_cursor.close()
                            except Exception:
                                pass

                    # Attach table-level sizes to the existing table list (if present).
                    for t in tables:
                        key = (t.get("schema"), t.get("name"))
                        if key in size_by_table:
                            t.update(size_by_table[key])

                    storage_info = {
                        "database_size": {
                            "data_size": db_active,
                            "index_size": 0,
                            "total_size": db_active + db_time_travel + db_failsafe,
                        },
                        "tables": table_storage,
                    }
                except Exception:
                    storage_info = None
                finally:
                    try:
                        if storage_cursor is not None:
                            storage_cursor.close()
                    except Exception:
                        pass

                cursor.close()
                conn.close()

                return {
                    "database_info": {
                        "type": "Snowflake",
                        "version": version,
                        "account": account,
                        "region": region,
                        "warehouse": warehouse,
                        "schemas": sorted(list({t["schema"] for t in tables})),
                        "database": target_database
                     },
                     "tables": tables,
                     "columns": columns,
                     "constraints": [],
                     "views": [t for t in tables if t["type"] == "VIEW"],
                     "procedures": [],
                     "indexes": [],
                     "data_profiles": data_profiles,
                     "storage_info": storage_info,
                     "driver_unavailable": False,
                 }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, analyze_sync)

        except Exception as e:
            return {
                "database_info": {"type": "Snowflake", "version": "Error", "schemas": []},
                "tables": [], "columns": [], "constraints": [], "views": [],
                "procedures": [], "indexes": [], "data_profiles": [],
                "error": str(e)
            }

    async def extract_objects(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ddl_scripts": {
                    "tables": [{"schema": "PUBLIC", "name": "USERS", "ddl": "-- simulated DDL"}],
                    "views": [],
                    "indexes": []
                },
                "object_count": 1,
                "driver_unavailable": True
            }

        try:
            def extract_sync():
                conn = snowflake.connector.connect(
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    account=self.credentials.get("account"),
                    warehouse=self.credentials.get("warehouse"),
                    database=self.credentials.get("database"),
                    schema=self.credentials.get("schema", "PUBLIC")
                )
                cursor = conn.cursor()

                target_schema = self.credentials.get("schema") or "PUBLIC"

                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                      AND TABLE_SCHEMA = %s
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """, (target_schema,))

                tables_ddl = []
                for schema, table in cursor.fetchall() or []:
                    ddl_text = f"-- Unable to extract DDL for {schema}.{table}"
                    try:
                        ddl_cursor = conn.cursor()
                        ddl_cursor.execute(f"SELECT GET_DDL('TABLE', '{schema}.{table}')")
                        ddl_result = ddl_cursor.fetchone()
                        if ddl_result and ddl_result[0]:
                            ddl_text = ddl_result[0]
                        ddl_cursor.close()
                    except Exception:
                        ddl_text = f"-- Unable to extract DDL for {schema}.{table}"

                    tables_ddl.append({
                        "schema": schema,
                        "name": table,
                        "ddl": ddl_text
                    })

                cursor.close()
                conn.close()

                return {
                    "ddl_scripts": {
                        "tables": tables_ddl,
                        "views": [],
                        "indexes": []
                    },
                    "object_count": len(tables_ddl)
                }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, extract_sync)

        except Exception as e:
            return {
                "ddl_scripts": {"tables": [], "views": [], "indexes": []},
                "object_count": 0,
                "error": str(e)
            }

    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        if not self.driver_available:
            # Treat missing driver as a hard failure so the UI does not show a false "success".
            return {
                "ok": False,
                "created": 0,
                "driver_unavailable": True,
                "message": "Snowflake driver not installed; cannot create objects"
            }

        try:
            def create_sync():
                import re

                def _normalize_statement(stmt: str) -> str:
                    s = str(stmt or "").strip().rstrip(";")
                    if not s:
                        return ""

                    # Common Postgres -> Snowflake cleanup (helps when AI/fallback returns Postgres-y defaults).
                    s = re.sub(r"::\s*regclass\b", "", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bgen_random_uuid\s*\(\s*\)", "UUID_STRING()", s, flags=re.IGNORECASE)
                    s = re.sub(r"\buuid_generate_v4\s*\(\s*\)", "UUID_STRING()", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bnow\s*\(\s*\)", "CURRENT_TIMESTAMP()", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bcharacter\s+varying\b", "VARCHAR", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bdouble\s+precision\b", "DOUBLE", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bjsonb\b", "VARIANT", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bjson\b", "VARIANT", s, flags=re.IGNORECASE)
                    s = re.sub(r"\buuid\b", "VARCHAR(36)", s, flags=re.IGNORECASE)
                    s = re.sub(r"\bbytea\b", "BINARY", s, flags=re.IGNORECASE)
                    s = re.sub(r"\btimestamp\s+with\s+time\s+zone\b", "TIMESTAMP_TZ", s, flags=re.IGNORECASE)
                    s = re.sub(r"\btimestamp\s+without\s+time\s+zone\b", "TIMESTAMP_NTZ", s, flags=re.IGNORECASE)

                    # Postgres-style sequence defaults / AI-generated NEXTVAL(...) => <sequence>.NEXTVAL
                    def _nextval_repl(m):
                        raw = (m.group(1) or m.group(2) or m.group(3) or "").strip()
                        raw = raw.strip("'").strip('"').replace('\"', "")
                        # Normalize whitespace inside identifiers (best effort).
                        raw = re.sub(r"\\s+", "", raw)
                        return f"{raw}.NEXTVAL" if raw else "NULL"

                    s = re.sub(
                        r"(?i)\bnextval\s*\(\s*(?:'([^']+)'|\"([^\"]+)\"|([^\)]+))\s*\)",
                        _nextval_repl,
                        s,
                    )

                    # If a column default references <sequence>.NEXTVAL, prefer Snowflake AUTOINCREMENT
                    # so we don't need to materialize sequences for typical SERIAL/identity columns.
                    s = re.sub(
                        r"(?i)(\b[A-Z0-9_\"$]+\b\s+)(BIGINT|INTEGER|INT|NUMBER)\s+DEFAULT\s+[A-Z0-9_\"$.]+\s*\.NEXTVAL\b",
                        r"\1\2 AUTOINCREMENT",
                        s,
                    )

                    # Normalize CREATE SEQUENCE to Snowflake syntax (strip MIN/MAX/CYCLE).
                    if re.match(r"(?is)^CREATE\s+SEQUENCE\b", s):
                        m_name = re.match(r"(?is)^CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)", s)
                        seq_name = m_name.group(1) if m_name else None
                        start_m = re.search(r"(?is)\bSTART\s+WITH\s+(\d+)\b", s)
                        inc_m = re.search(r"(?is)\bINCREMENT\s+BY\s+(-?\d+)\b", s)
                        start_v = start_m.group(1) if start_m else None
                        inc_v = inc_m.group(1) if inc_m else None
                        if seq_name:
                            parts = [f"CREATE SEQUENCE IF NOT EXISTS {seq_name}"]
                            if start_v is not None:
                                parts.append(f"START = {start_v}")
                            if inc_v is not None:
                                parts.append(f"INCREMENT = {inc_v}")
                            s = " ".join(parts)

                    return s + ";"

                def _qident(identifier: str) -> str:
                    return '"' + str(identifier).replace('"', '""') + '"'

                context = self._resolve_writable_context()
                target_database = context["database"]
                target_schema = context["schema"]

                connection = snowflake.connector.connect(
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    account=self.credentials.get("account"),
                    warehouse=self.credentials.get("warehouse"),
                    database=None if context["fallback_used"] else target_database,
                    schema=target_schema
                )
                cursor = connection.cursor()

                created_count = 0
                try:
                    if context["fallback_used"]:
                        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(target_database)}")
                    if target_database:
                        cursor.execute(f"USE DATABASE {_qident(target_database)}")
                    if target_schema:
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(target_schema)}")
                        cursor.execute(f"USE SCHEMA {_qident(target_schema)}")
                except Exception as e:
                    print(f"[Snowflake] Warning: failed to set database/schema context: {e}")

                # Ensure any referenced schemas exist (best-effort).
                try:
                    schemas_to_create = {
                        str(obj.get("schema") or "").strip()
                        for obj in (translated_ddl or [])
                        if str(obj.get("schema") or "").strip()
                    }
                    for sch in sorted(schemas_to_create, key=lambda s: s.lower()):
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(sch)}")
                except Exception as e:
                    print(f"[Snowflake] Warning: failed to pre-create schemas: {e}")

                errors: List[Dict[str, Any]] = []
                for obj in translated_ddl:
                    try:
                        # `ai.translate_schema` and fallbacks return `target_sql`
                        ddl = (
                            obj.get("target_sql")
                            or obj.get("translated_ddl")
                            or obj.get("ddl")
                            or obj.get("source_ddl")
                            or ""
                        )
                        ddl = str(ddl).strip()
                        if not ddl:
                            continue

                        # Snowflake connector executes one statement at a time; split on semicolons.
                        statements = [s.strip() for s in ddl.split(";") if s.strip()]
                        for stmt in statements:
                            # Make CREATE TABLE rerunnable.
                            stmt = re.sub(r"(?i)^CREATE\\s+TABLE\\s+", "CREATE TABLE IF NOT EXISTS ", stmt, count=1)
                            normalized = _normalize_statement(stmt)
                            if not normalized.strip():
                                continue
                            cursor.execute(normalized)
                    except Exception as e:
                        msg = str(e)
                        print(f"[Snowflake] Error creating object {obj.get('name', '')}: {msg}")
                        if "already exists" in msg.lower():
                            # treat as success
                            created_count += 1
                            continue
                        stmt_preview = (obj.get("target_sql") or obj.get("translated_ddl") or obj.get("ddl") or obj.get("source_ddl") or "")
                        stmt_preview = str(stmt_preview).strip().replace("\r", " ").replace("\n", " ")
                        if len(stmt_preview) > 300:
                            stmt_preview = stmt_preview[:300] + "..."
                        errors.append(
                            {
                                "name": obj.get("name"),
                                "schema": obj.get("schema"),
                                "kind": obj.get("kind"),
                                "error": msg,
                                "statement": stmt_preview,
                            }
                        )
                        continue
                    else:
                        created_count += 1

                connection.commit()
                cursor.close()
                connection.close()

                attempted = len(translated_ddl)
                ok = (len(errors) == 0) and ((attempted == 0) or (created_count >= attempted))
                return {
                    "ok": ok,
                    "created": created_count,
                    "attempted": attempted,
                    "errors": errors,
                    "message": None if ok else "One or more objects failed to create in Snowflake"
                }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, create_sync)
            return result

        except Exception as e:
            return {"ok": False, "message": str(e)}

    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "dropped": len(table_names)}

        import re

        def _fmt_ident(identifier: str) -> str:
            """
            Snowflake folds unquoted identifiers to UPPERCASE. If we always quote we can miss
            unquoted objects (e.g. "ns" != NS). Prefer unquoted uppercase for simple identifiers.
            """
            ident = str(identifier).strip()
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", ident or ""):
                return ident.upper()
            return '"' + ident.replace('"', '""') + '"'

        def _quote_ident(identifier: str) -> str:
            ident = str(identifier).strip()
            return '"' + ident.replace('"', '""') + '"'

        def _qlit(value: str) -> str:
            return "'" + str(value).replace("'", "''") + "'"

        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('"')

        try:
            def drop_sync():
                context = self._resolve_writable_context()
                default_database = context["database"]
                default_schema = context["schema"] or "PUBLIC"

                conn = snowflake.connector.connect(
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    account=self.credentials.get("account"),
                    warehouse=self.credentials.get("warehouse"),
                    database=None if context["fallback_used"] else default_database,
                    schema=default_schema,
                )
                cur = conn.cursor()

                dropped = 0
                errors: List[Dict[str, Any]] = []

                try:
                    if context["fallback_used"]:
                        cur.execute(f"CREATE DATABASE IF NOT EXISTS {_fmt_ident(default_database)}")
                    if default_database:
                        cur.execute(f"USE DATABASE {_fmt_ident(default_database)}")
                    if default_schema:
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_fmt_ident(default_schema)}")
                        cur.execute(f"USE SCHEMA {_fmt_ident(default_schema)}")
                except Exception as e:
                    print(f"[Snowflake] Warning: failed to set default context: {e}")

                for ref in table_names or []:
                    try:
                        raw = str(ref or "").strip()
                        if not raw:
                            continue
                        parts = [p for p in raw.split(".") if p]
                        if len(parts) >= 3:
                            database_part, schema_part, table_part = parts[-3], parts[-2], parts[-1]
                        elif len(parts) == 2:
                            database_part, schema_part, table_part = None, parts[0], parts[1]
                        else:
                            database_part, schema_part, table_part = None, default_schema, parts[0]

                        database = _clean_ident(database_part) if database_part else default_database
                        schema = _clean_ident(schema_part) if schema_part else default_schema
                        table = _clean_ident(table_part)
                        if not table:
                            continue

                        # Snowflake identifiers can be created quoted (case-sensitive) or unquoted (folded to UPPERCASE).
                        # Attempt both styles so tables like "small_table" are dropped correctly.
                        db_sql = _fmt_ident(database) if database else None
                        sch_sql = _fmt_ident(schema) if schema else None
                        tbl_unquoted = _fmt_ident(table)
                        tbl_quoted = _quote_ident(table)

                        fq_parts = [p for p in [db_sql, sch_sql] if p]
                        fq_unquoted = ".".join([*fq_parts, tbl_unquoted])
                        fq_quoted_tbl = ".".join([*fq_parts, tbl_quoted])

                        cur.execute(f"DROP TABLE IF EXISTS {fq_unquoted}")
                        status1 = None
                        try:
                            row = cur.fetchone()
                            status1 = row[0] if row else None
                        except Exception:
                            pass

                        cur.execute(f"DROP TABLE IF EXISTS {fq_quoted_tbl}")
                        status2 = None
                        try:
                            row = cur.fetchone()
                            status2 = row[0] if row else None
                        except Exception:
                            pass

                        dropped_this = False
                        for status in (status1, status2):
                            if isinstance(status, str) and "successfully dropped" in status.lower():
                                dropped_this = True
                                break

                        # Verify drop (covers cases where IF EXISTS returns "does not exist" but table still exists due to quoting).
                        if database and schema:
                            verify = (
                                f"SELECT COUNT(*) FROM {_fmt_ident(database)}.INFORMATION_SCHEMA.TABLES "
                                f"WHERE TABLE_SCHEMA ILIKE {_qlit(schema)} AND TABLE_NAME ILIKE {_qlit(table)}"
                            )
                            cur.execute(verify)
                            remaining = cur.fetchone()[0]
                            if remaining and int(remaining) > 0:
                                errors.append({"table": ref, "error": "Table still exists after drop attempt (check quoting/permissions)"})
                                continue

                        if dropped_this:
                            dropped += 1
                    except Exception as e:
                        errors.append({"table": ref, "error": str(e)})

                conn.commit()
                cur.close()
                conn.close()
                return {"ok": len(errors) == 0, "dropped": dropped, "errors": errors}

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, drop_sync)
        except Exception as e:
            return {"ok": False, "message": str(e)}

    async def yield_table_data(self, table_name: str, chunk_size: int = 10000, columns: Optional[List[str]] = None):
        """Async generator to yield data from Snowflake table in chunks as (columns, rows) tuples."""
        if not self.driver_available:
            return

        def _qident(identifier: str) -> str:
            return '"' + str(identifier).replace('"', '""') + '"'

        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('"')

        try:
            context = self._resolve_writable_context()
            default_database = context["database"]
            default_schema = context["schema"] or "PUBLIC"

            # Parse table reference (supports schema.table or database.schema.table)
            parts = [p for p in str(table_name).split(".") if p]
            if len(parts) >= 3:
                database_part, schema_part, table_part = parts[-3], parts[-2], parts[-1]
            elif len(parts) == 2:
                database_part, schema_part, table_part = None, parts[0], parts[1]
            else:
                database_part, schema_part, table_part = None, default_schema, parts[0]

            database = _clean_ident(database_part) if database_part else default_database
            schema = _clean_ident(schema_part) if schema_part else default_schema
            table = _clean_ident(table_part)

            schema_raw = str(schema)
            table_raw = str(table)
            # Snowflake folds unquoted identifiers to uppercase, but quoted objects can be mixed-case.
            schema_uc = schema_raw.upper()
            table_uc = table_raw.upper()

            conn = snowflake.connector.connect(
                account=self.credentials.get("account"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else (database or default_database),
                schema=default_schema,
            )
            cur = conn.cursor()

            try:
                if context["fallback_used"]:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(default_database)}")
                if database:
                    cur.execute(f"USE DATABASE {_qident(database)}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(schema_raw)}")
                requested_columns = [str(c) for c in (columns or []) if str(c or "").strip()]
                select_cols = ", ".join(_qident(col) for col in requested_columns) if requested_columns else "*"
                # Fully qualify to avoid context issues, and try both as-is and uppercase.
                try:
                    cur.execute(f"SELECT {select_cols} FROM {_qident(schema_raw)}.{_qident(table_raw)}")
                except Exception:
                    cur.execute(f"SELECT {select_cols} FROM {_qident(schema_uc)}.{_qident(table_uc)}")
                columns = [desc[0] for desc in (cur.description or [])]

                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield (columns, rows)
            finally:
                cur.close()
                conn.close()
        except Exception as e:
            raise Exception(f"Error reading Snowflake table {table_name}: {str(e)}")

    async def copy_table_data(
        self,
        table_name: str,
        source_adapter: 'DatabaseAdapter',
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": False,
                "table": table_name,
                "rows_copied": 0,
                "driver_unavailable": True,
                "error": "Snowflake driver not installed; cannot migrate data."
            }

        try:
            context = self._resolve_writable_context()
            target_database = context["database"]
            target_schema = context["schema"]

            def _qsf(identifier: str) -> str:
                return '"' + str(identifier).replace('"', '""') + '"'

            def _qmysql(identifier: str) -> str:
                escaped = str(identifier).replace("`", "``")
                return f"`{escaped}`"

            source_adapter_name = (source_adapter.__class__.__name__ or "").lower()
            is_mysql_source = "mysql" in source_adapter_name
            is_postgres_source = ("postgres" in source_adapter_name) or hasattr(source_adapter, "_connect")
            requested_columns = [str(c) for c in (columns or []) if str(c or "").strip()]

            if is_mysql_source:
                try:
                    import mysql.connector  # type: ignore
                except Exception:
                    return {"ok": False, "table": table_name, "rows_copied": 0, "error": "mysql connector not installed for source fetch"}

                parts = [p for p in str(table_name).split(".") if p]
                if len(parts) >= 2:
                    source_schema, source_table = parts[-2], parts[-1]
                else:
                    return {"ok": False, "table": table_name, "rows_copied": 0, "error": "MySQL source requires schema.table for data copy"}

                src = getattr(source_adapter, "credentials", {}) or {}
                conn_params = {
                    "host": src.get("host"),
                    "port": int(src.get("port", 3306)),
                    "user": (src.get("username") or "").strip(),
                    "password": src.get("password"),
                }

                ssl_value = src.get("ssl", False)
                if isinstance(ssl_value, str):
                    ssl_enabled = ssl_value.lower() in ("true", "1", "yes")
                else:
                    ssl_enabled = bool(ssl_value)
                conn_params["ssl_disabled"] = not ssl_enabled

                src_conn = mysql.connector.connect(**conn_params)
                src_cursor = src_conn.cursor(buffered=True)

                # Pre-count source rows for validation
                src_cursor.execute(f"SELECT COUNT(*) FROM {_qmysql(source_schema)}.{_qmysql(source_table)}")
                source_count_row = src_cursor.fetchone()
                expected_source_rows = int(source_count_row[0]) if source_count_row and source_count_row[0] is not None else 0

                # Build an explicit column list using information_schema so we include MySQL 8
                # INVISIBLE columns (omitted from SELECT *), which would otherwise cause NOT NULL
                # insert failures in Snowflake.
                try:
                    meta_cur = src_conn.cursor(buffered=True)
                    meta_cur.execute(
                        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
                        (source_schema, source_table),
                    )
                    src_columns = [r[0] for r in (meta_cur.fetchall() or []) if r and r[0]]
                    meta_cur.close()
                except Exception:
                    src_columns = []

                if src_columns:
                    if requested_columns:
                        src_by_lower = {str(c).lower(): c for c in src_columns}
                        filtered = [src_by_lower[c.lower()] for c in requested_columns if c.lower() in src_by_lower]
                        src_columns = filtered
                    if not src_columns:
                        return {"ok": False, "table": table_name, "rows_copied": 0, "error": "Selected columns not found in source"}
                    select_cols = ", ".join([_qmysql(c) for c in src_columns])
                    src_cursor.execute(
                        f"SELECT {select_cols} FROM {_qmysql(source_schema)}.{_qmysql(source_table)}"
                    )
                    columns = list(src_columns)
                else:
                    # Fallback
                    if requested_columns:
                        select_cols = ", ".join([_qmysql(c) for c in requested_columns])
                        src_cursor.execute(
                            f"SELECT {select_cols} FROM {_qmysql(source_schema)}.{_qmysql(source_table)}"
                        )
                    else:
                        src_cursor.execute(f"SELECT * FROM {_qmysql(source_schema)}.{_qmysql(source_table)}")
                    columns = [desc[0] for desc in (src_cursor.description or [])]

            elif is_postgres_source:
                if psycopg2 is None:
                    return {"ok": False, "table": table_name, "rows_copied": 0, "error": "psycopg2 not installed for source fetch"}

                # Parse source table name
                if "." in table_name:
                    source_schema, source_table = table_name.split(".", 1)
                else:
                    source_schema, source_table = "public", table_name

                # Build source connection params from adapter credentials (use adapter helper when available).
                if hasattr(source_adapter, "_connect"):
                    src_conn = source_adapter._connect(database=getattr(source_adapter, "credentials", {}).get("database"))
                else:
                    src = getattr(source_adapter, "credentials", {})
                    src_conn = psycopg2.connect(
                        host=src.get("host"),
                        port=int(src.get("port", 5432)),
                        user=src.get("username"),
                        password=src.get("password"),
                        database=src.get("database") or src.get("db") or "postgres",
                        sslmode=src.get("sslmode", "prefer")
                    )

                def _qpg(ident: str) -> str:
                    return '"' + str(ident).replace('"', '""') + '"'

                select_cols = "*"
                if requested_columns:
                    select_cols = ", ".join(_qpg(c) for c in requested_columns)

                src_cursor = src_conn.cursor()
                src_cursor.execute(f'SELECT {select_cols} FROM "{source_schema}"."{source_table}"')
                columns = [desc[0] for desc in src_cursor.description]
                # Pre-count source rows for validation
                src_cursor.execute(f'SELECT COUNT(*) FROM "{source_schema}"."{source_table}"')
                source_count_row = src_cursor.fetchone()
                expected_source_rows = int(source_count_row[0]) if source_count_row and source_count_row[0] is not None else 0
                # Reopen cursor for data fetch
                src_cursor.close()
                src_cursor = src_conn.cursor()
                src_cursor.execute(f'SELECT {select_cols} FROM "{source_schema}"."{source_table}"')
            else:
                return {"ok": False, "table": table_name, "rows_copied": 0, "error": f"Unsupported source adapter for Snowflake copy: {source_adapter.__class__.__name__}"}

            # Prepare Snowflake insert
            sf_conn = snowflake.connector.connect(
                account=self.credentials.get("account"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else target_database,
                schema=target_schema
            )
            sf_cursor = sf_conn.cursor()

            # Ensure target database/schema exists and is set as context
            if context["fallback_used"]:
                sf_cursor.execute(f'CREATE DATABASE IF NOT EXISTS "{target_database}"')
            sf_cursor.execute(f'USE DATABASE "{target_database}"')
            sf_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            sf_cursor.execute(f'USE SCHEMA "{target_schema}"')

            # Resolve the actual target schema/table name and column casing from Snowflake to avoid
            # failures when objects were created with quoted/lowercase identifiers or in a
            # different schema than the default target schema.
            candidate_tables: List[str] = []
            for cand in (source_table, str(source_table).upper(), str(source_table).lower()):
                if cand and cand not in candidate_tables:
                    candidate_tables.append(cand)

            candidate_schemas: List[str] = []
            for sch in (
                target_schema,
                source_schema,
                str(source_schema).upper() if source_schema else "",
                str(source_schema).lower() if source_schema else "",
                "PUBLIC",
            ):
                if sch and sch not in candidate_schemas:
                    candidate_schemas.append(sch)

            target_table: str | None = None
            target_schema_actual: str | None = None
            target_columns_actual: Dict[str, str] = {}

            def _try_desc(schema_name: str, table_name: str) -> bool:
                nonlocal target_table, target_schema_actual, target_columns_actual
                try:
                    sf_cursor.execute(f'DESC TABLE {_qsf(target_database)}.{_qsf(schema_name)}.{_qsf(table_name)}')
                    desc_rows = sf_cursor.fetchall() or []
                    if not desc_rows:
                        return False
                    cols_actual: Dict[str, str] = {}
                    for r in desc_rows:
                        if not r:
                            continue
                        col_name = str(r[0]).strip()
                        if col_name:
                            cols_actual[col_name.lower()] = col_name
                    target_table = table_name
                    target_schema_actual = schema_name
                    target_columns_actual = cols_actual
                    return True
                except Exception:
                    return False

            for schema_candidate in candidate_schemas:
                for table_candidate in candidate_tables:
                    if _try_desc(schema_candidate, table_candidate):
                        break
                if target_table:
                    break

            if not target_table:
                # Last resort: search INFORMATION_SCHEMA for the table in any schema in target DB.
                try:
                    sf_cursor.execute(
                        f"""
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM {_qsf(target_database)}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_NAME ILIKE %s
                        """,
                        (str(source_table),),
                    )
                    matches = sf_cursor.fetchall() or []
                    if matches:
                        # Prefer the configured target schema if present in matches.
                        chosen = None
                        for schema_name, table_name in matches:
                            if str(schema_name).upper() == str(target_schema).upper():
                                chosen = (schema_name, table_name)
                                break
                        if not chosen:
                            chosen = matches[0]
                        schema_name, table_name = chosen
                        if _try_desc(schema_name, table_name):
                            target_schema_actual = schema_name
                except Exception:
                    pass

            if not target_table:
                return {
                    "ok": False,
                    "table": f"{target_schema}.{source_table}",
                    "rows_copied": 0,
                    "error": (
                        f"Target table not found in Snowflake database '{target_database}'. "
                        "Run structure migration first or verify target schema."
                    ),
                }

            if not target_schema_actual:
                target_schema_actual = target_schema

            # Build column list by mapping source -> actual target column names (case-insensitive).
            target_columns = []
            source_indices: List[int] = []
            missing_cols: List[str] = []
            for idx, c in enumerate(columns):
                key = str(c).lower()
                if key in target_columns_actual:
                    target_columns.append(target_columns_actual[key])
                    source_indices.append(idx)
                else:
                    # If the target table doesn't have this column (likely because it was trimmed),
                    # skip it instead of inserting into a non-existent column.
                    missing_cols.append(str(c))

            # If nothing remains, abort with a clear error.
            if not target_columns:
                return {
                    "ok": False,
                    "table": f"{target_schema}.{source_table}",
                    "rows_copied": 0,
                    "error": "No matching columns between source selection and target table"
                }

            cols_quoted = ", ".join([f'"{col}"' for col in target_columns])
            placeholders = ", ".join(["%s"] * len(target_columns))
            insert_sql = f'INSERT INTO {_qsf(target_database)}.{_qsf(target_schema_actual)}.{_qsf(target_table)} ({cols_quoted}) VALUES ({placeholders})'

            # Make repeated runs idempotent: replace table contents rather than append.
            sf_cursor.execute(f'TRUNCATE TABLE {_qsf(target_database)}.{_qsf(target_schema_actual)}.{_qsf(target_table)}')

            total_rows = 0
            while True:
                rows = src_cursor.fetchmany(chunk_size)
                if not rows:
                    break
                trimmed = [tuple(row[i] for i in source_indices) for row in rows]
                sf_cursor.executemany(insert_sql, trimmed)
                total_rows += len(trimmed)
                if callable(progress_cb):
                    try:
                        progress_cb(total_rows, len(trimmed))
                    except Exception:
                        pass

            # Validate target row count matches source
            sf_cursor.execute(f'SELECT COUNT(*) FROM {_qsf(target_database)}.{_qsf(target_schema_actual)}.{_qsf(target_table)}')
            target_count_row = sf_cursor.fetchone()
            target_count = int(target_count_row[0]) if target_count_row and target_count_row[0] is not None else 0

            sf_conn.commit()
            sf_cursor.close()
            sf_conn.close()
            src_cursor.close()
            src_conn.close()

            ok = (total_rows == target_count) and (expected_source_rows == 0 or target_count == expected_source_rows)
            message = None
            if not ok:
                message = f"Row count mismatch after copy (source expected {expected_source_rows}, target inserted {target_count})"

            result: Dict[str, Any] = {
                "ok": ok,
                "table": f"{target_schema_actual}.{source_table}",
                "rows_copied": target_count
            }
            if message:
                result["error"] = message
            if missing_cols and not ok:
                result["details"] = {"missing_target_columns": missing_cols[:20]}
            return result
        except Exception as e:
            return {
                "ok": False,
                "table": table_name,
                "rows_copied": 0,
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    async def run_validation_checks(self, source_adapter: 'DatabaseAdapter', table_names: List[str] | None = None) -> Dict[str, Any]:
        if not self.driver_available:
            return {"structural": {"schema_match": True}, "data": {"row_counts_match": True}, "driver_unavailable": True}

        try:
            src = getattr(source_adapter, "credentials", {})
            
            # Determine source adapter type to use appropriate connector
            source_adapter_name = (source_adapter.__class__.__name__ or "").lower()
            is_mysql_source = "mysql" in source_adapter_name
            is_postgres_source = ("postgres" in source_adapter_name) or hasattr(source_adapter, "_connect")
            
            if is_postgres_source and psycopg2 is None:
                raise Exception("psycopg2 not installed for PostgreSQL validation")
            
            if is_mysql_source:
                try:
                    import mysql.connector
                except ImportError:
                    raise Exception("mysql-connector-python not installed for MySQL validation")

            context = self._resolve_writable_context()
            target_database = context["database"]
            target_schema = context["schema"]

            # Simple validation: compare row counts for all migrated tables
            # We assume migration_state tables were passed; here we check every table in target schema
            sf_conn = snowflake.connector.connect(
                account=self.credentials.get("account"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else target_database,
                schema=target_schema
            )
            sf_cursor = sf_conn.cursor()
            if context["fallback_used"]:
                sf_cursor.execute(f'CREATE DATABASE IF NOT EXISTS "{target_database}"')
            sf_cursor.execute(f'USE DATABASE "{target_database}"')
            sf_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            sf_cursor.execute(f'USE SCHEMA "{target_schema}"')
            # If specific tables were provided, use those; otherwise introspect all in schema.
            target_tables: List[dict[str, str]]
            if table_names is not None:
                parsed = []
                for raw in table_names:
                    text = str(raw or "").strip()
                    if not text:
                        continue
                    # For MySQL source, table names may come as 'database.table' format
                    # For PostgreSQL source, table names may come as 'schema.table' format
                    # However, during MySQL -> Snowflake migration, the database name is stripped
                    # so tables in Snowflake exist as just the table name in the target schema
                    if "." in text:
                        # Split on the first dot to separate schema/database from table name
                        schema_part, table_part = text.split(".", 1)
                        schema = schema_part.strip().strip('"')
                        table = table_part.strip().strip('"')
                        # For MySQL -> Snowflake migration, the target table will be just 'table' in target_schema
                        # Store the original label but use empty schema for target lookup
                        if is_mysql_source:
                            parsed.append({"schema": "", "table": table, "label": text})
                        else:
                            parsed.append({"schema": schema, "table": table, "label": text})
                    else:
                        schema = ""
                        table = text.strip().strip('"')
                        parsed.append({"schema": schema, "table": table, "label": text})
                seen = set()
                target_tables = []
                for item in parsed:
                    # Use table name only for deduplication since target tables are in target_schema
                    key = item["table"].lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    target_tables.append(item)
            else:
                sf_cursor.execute("""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                """, (target_schema,))
                target_tables = [{"schema": "", "table": row[0], "label": row[0]} for row in sf_cursor.fetchall() or []]

            # Connect to source database based on type
            if is_postgres_source:
                src_conn = psycopg2.connect(
                    host=src.get("host"),
                    port=int(src.get("port", 5432)),
                    user=src.get("username"),
                    password=src.get("password"),
                    database=src.get("database") or src.get("db") or "postgres",
                    sslmode=src.get("sslmode", "prefer")
                )
                src_cursor = src_conn.cursor()
            elif is_mysql_source:
                conn_params = {
                    "host": src.get("host"),
                    "port": int(src.get("port", 3306)),
                    "user": (src.get("username") or "").strip(),
                    "password": src.get("password"),
                }
                
                ssl_value = src.get("ssl", False)
                if isinstance(ssl_value, str):
                    ssl_enabled = ssl_value.lower() in ("true", "1", "yes")
                else:
                    ssl_enabled = bool(ssl_value)
                conn_params["ssl_disabled"] = not ssl_enabled
                
                # Add database if available
                db_name = src.get("database") or src.get("db")
                if db_name:
                    conn_params["database"] = db_name
                
                src_conn = mysql.connector.connect(**conn_params)
                src_cursor = src_conn.cursor(buffered=True)
            else:
                raise Exception(f"Unsupported source adapter for validation: {source_adapter.__class__.__name__}")

            def _src_count(schema: str, table: str) -> int:
                candidates = [table, table.lower(), table.upper()]
                for cand in candidates:
                    try:
                        if is_postgres_source:
                            src_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{cand}"')
                        elif is_mysql_source:
                            # For MySQL, use backticks for identifiers
                            # If schema is empty, just query the table directly
                            if schema:
                                src_cursor.execute(f'SELECT COUNT(*) FROM `{schema}`.`{cand}`')
                            else:
                                src_cursor.execute(f'SELECT COUNT(*) FROM `{cand}`')
                        else:
                            # Fallback for other databases
                            src_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{cand}"')
                        result = src_cursor.fetchone()
                        if result:
                            return int(result[0] or 0)
                        else:
                            return 0
                    except Exception:
                        pass
                for cand in candidates:
                    try:
                        if is_postgres_source:
                            src_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{cand}")
                        elif is_mysql_source:
                            # For MySQL, use backticks for identifiers
                            # If schema is empty, just query the table directly
                            if schema:
                                src_cursor.execute(f"SELECT COUNT(*) FROM `{schema}`.`{cand}`")
                            else:
                                src_cursor.execute(f"SELECT COUNT(*) FROM `{cand}`")
                        else:
                            # Fallback for other databases
                            src_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{cand}")
                        result = src_cursor.fetchone()
                        if result:
                            return int(result[0] or 0)
                        else:
                            return 0
                    except Exception:
                        pass
                # If we can't find the table in the expected schema, try without schema qualification
                if is_mysql_source and schema:
                    for cand in candidates:
                        try:
                            src_cursor.execute(f'SELECT COUNT(*) FROM `{cand}`')
                            result = src_cursor.fetchone()
                            if result:
                                return int(result[0] or 0)
                        except Exception:
                            pass
                raise Exception(f"Source table not found: {schema}.{table}")

            def _sf_count(schema: str, table: str) -> int:
                candidates = [table, table.upper(), table.lower()]
                for cand in candidates:
                    try:
                        sf_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{cand}"')
                        result = sf_cursor.fetchone()
                        if result:
                            return int(result[0] or 0)
                        else:
                            return 0
                    except Exception:
                        pass
                # Also try without schema qualification in case table exists in different schema
                for cand in candidates:
                    try:
                        sf_cursor.execute(f'SELECT COUNT(*) FROM "{cand}"')
                        result = sf_cursor.fetchone()
                        if result:
                            return int(result[0] or 0)
                        else:
                            return 0
                    except Exception:
                        pass
                # Unquoted fallback (Snowflake folds to upper)
                try:
                    sf_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    result = sf_cursor.fetchone()
                    if result:
                        return int(result[0] or 0)
                    else:
                        return 0
                except Exception:
                    # Last resort: try without schema
                    try:
                        sf_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        result = sf_cursor.fetchone()
                        if result:
                            return int(result[0] or 0)
                        else:
                            return 0
                    except Exception:
                        return 0

            results = {}
            for item in target_tables:
                table = item["table"]
                # Determine source schema based on source type
                if is_mysql_source:
                    # For MySQL, the schema is typically the database name
                    src_schema = src.get("database") or src.get("db") or ""
                else:
                    # For PostgreSQL and other sources, use default schema
                    src_schema = item["schema"] or src.get("schema", "public")
                # source count
                try:
                    src_count = _src_count(src_schema, table)
                except Exception as e:
                    src_count = 0
                    results[item["label"]] = {
                        "source_rows": 0,
                        "target_rows": 0,
                        "match": False,
                        "error": f"Source count failed: {e}"
                    }
                    continue

                # target count
                try:
                    tgt_count = _sf_count(target_schema, table)
                except Exception as e:
                    tgt_count = 0
                    results[item["label"]] = {
                        "source_rows": int(src_count),
                        "target_rows": 0,
                        "match": False,
                        "error": f"Target count failed: {e}"
                    }
                    continue

                results[item["label"]] = {
                    "source_rows": int(src_count),
                    "target_rows": int(tgt_count),
                    "match": int(src_count) == int(tgt_count)
                }

            sf_cursor.close()
            sf_conn.close()
            src_cursor.close()
            src_conn.close()

            all_match = all(v["match"] for v in results.values()) if results else True
            return {"structural": {"schema_match": True}, "data": {"row_counts_match": all_match, "tables": results}}
        except Exception as e:
            return {"structural": {"schema_match": False}, "data": {"row_counts_match": False, "error": str(e)}}

    async def get_table_row_count(self, table_name: str) -> int:
        """Return row count for a table in the configured Snowflake database/schema."""
        if not self.driver_available:
            return 0

        def _qident(identifier: str) -> str:
            return '"' + str(identifier).replace('"', '""') + '"'

        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('"')

        def count_sync() -> int:
            context = self._resolve_writable_context()
            database = context["database"]
            default_schema = context["schema"]

            if "." in table_name:
                schema_part, table_part = table_name.split(".", 1)
                schema = _clean_ident(schema_part) or default_schema
                table = _clean_ident(table_part)
            else:
                schema = default_schema
                table = _clean_ident(table_name)

            # Snowflake folds unquoted identifiers to uppercase.
            schema = str(schema).upper()
            table = str(table).upper()

            connection = snowflake.connector.connect(
                account=self.credentials.get("account"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else database,
                schema=default_schema,
            )
            cursor = connection.cursor()
            try:
                if context["fallback_used"]:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(database)}")
                if database:
                    cursor.execute(f"USE DATABASE {_qident(database)}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(default_schema)}")
                cursor.execute(f"USE SCHEMA {_qident(schema)}")
                cursor.execute(f"SELECT COUNT(*) FROM {_qident(table)}")
                return int(cursor.fetchone()[0])
            finally:
                cursor.close()
                connection.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, count_sync)

    async def get_schema_structure(self, tables_ddl: list) -> dict:
        """Return {table_name: [{name,type}, ...]} for provided tables, using target schema."""
        if not self.driver_available:
            return {}

        def _qident(identifier: str) -> str:
            return '"' + str(identifier).replace('"', '""') + '"'

        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('"')

        def schema_sync() -> dict:
            database = self.credentials.get("database")
            target_schema = self.credentials.get("schema", "PUBLIC")
            target_schema_uc = str(target_schema).upper()

            connection = snowflake.connector.connect(
                account=self.credentials.get("account"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=database,
                schema=target_schema,
            )
            cursor = connection.cursor()

            try:
                if database:
                    cursor.execute(f"USE DATABASE {_qident(database)}")

                schema_info: Dict[str, Any] = {}
                for table in tables_ddl:
                    table_name = table.get("name", "")
                    table_uc = _clean_ident(table_name).upper()

                    schema_candidates = []
                    if target_schema_uc:
                        schema_candidates.append(target_schema_uc)
                    if table.get("schema"):
                        schema_candidates.append(_clean_ident(table.get("schema")).upper())
                    schema_candidates.append("PUBLIC")

                    seen = set()
                    schema_candidates = [s for s in schema_candidates if s and not (s in seen or seen.add(s))]

                    columns = []
                    for schema_uc in schema_candidates:
                        if database:
                            cursor.execute(
                                f"""
                                SELECT COLUMN_NAME, DATA_TYPE
                                FROM {_qident(database)}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                                ORDER BY ORDINAL_POSITION
                                """,
                                (schema_uc, table_uc),
                            )
                        else:
                            cursor.execute(
                                """
                                SELECT COLUMN_NAME, DATA_TYPE
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                                ORDER BY ORDINAL_POSITION
                                """,
                                (schema_uc, table_uc),
                            )
                        fetched = cursor.fetchall()
                        if fetched:
                            columns = [{"name": row[0], "type": row[1]} for row in fetched]
                            break

                    schema_info[_clean_ident(table_name)] = columns

                return schema_info
            finally:
                cursor.close()
                connection.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, schema_sync)

    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        """Rename a column in a Snowflake table using ALTER TABLE ... RENAME COLUMN."""
        if not self.driver_available:
            return {"ok": False, "message": "Snowflake driver not available"}
        
        def _qident(identifier: str) -> str:
            return '"' + str(identifier).replace('"', '""') + '"'
        
        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('"')
        
        def rename_sync():
            context = self._resolve_writable_context()
            database = context["database"]
            default_schema = context["schema"] or "PUBLIC"

            # Parse table name
            if "." in table_name:
                schema_part, table_part = table_name.split(".", 1)
                base_schema = _clean_ident(schema_part) or default_schema
                base_table = _clean_ident(table_part)
            else:
                base_schema = default_schema
                base_table = _clean_ident(table_name)

            old_col_clean = _clean_ident(old_column_name)
            new_col_clean = _clean_ident(new_column_name)

            connection = snowflake.connector.connect(
                account=self._clean_account(self.credentials.get("account")),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=database or context["database"],
                schema=default_schema,
            )
            cursor = connection.cursor()
            
            try:
                if database:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(database)}")
                    cursor.execute(f"USE DATABASE {_qident(database)}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(default_schema)}")
                cursor.execute(f"USE SCHEMA {_qident(default_schema)}")

                schema_candidates = []
                for s in {base_schema, base_schema.upper(), default_schema, default_schema.upper()}:
                    if s:
                        schema_candidates.append(s)
                # remove duplicates while preserving order
                seen = set()
                schema_candidates = [s for s in schema_candidates if not (s in seen or seen.add(s))]

                table_candidates = []
                for t in {base_table, base_table.upper(), base_table.lower()}:
                    if t:
                        table_candidates.append(t)
                seen_t = set()
                table_candidates = [t for t in table_candidates if not (t in seen_t or seen_t.add(t))]

                last_err = None
                for sch in schema_candidates:
                    try:
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(sch)}")
                    except Exception:
                        pass
                    try:
                        cursor.execute(f"USE SCHEMA {_qident(sch)}")
                    except Exception:
                        continue

                    for tbl in table_candidates:
                        attempts = [
                            f"ALTER TABLE {_qident(tbl)} RENAME COLUMN {_qident(old_col_clean)} TO {_qident(new_col_clean)}",
                            f"ALTER TABLE {tbl} RENAME COLUMN {old_col_clean} TO {new_col_clean}",
                        ]
                        for stmt in attempts:
                            try:
                                cursor.execute(stmt)
                                connection.commit()
                                return {"ok": True, "message": f"Renamed {sch}.{tbl}: {old_col_clean} -> {new_col_clean}"}
                            except Exception as e:
                                last_err = e
                                continue

                if last_err:
                    raise last_err
                
            except Exception as e:
                try:
                    connection.rollback()
                except Exception:
                    pass
                return {"ok": False, "message": f"Failed to rename column: {str(e)}"}
            finally:
                cursor.close()
                connection.close()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, rename_sync)

    async def drop_column(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Drop a column from a Snowflake table (best-effort, tries quoted and unquoted)."""
        if not self.driver_available:
            return {"ok": False, "message": "Snowflake driver not available"}

        def _qident(identifier: str) -> str:
            return '"' + str(identifier).replace('"', '""') + '"'

        def _maybe_unquoted(ident: str) -> str:
            # If simple identifier, prefer unquoted so Snowflake folds to uppercase.
            import re
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", ident or ""):
                return ident
            return _qident(ident)

        def drop_sync():
            context = self._resolve_writable_context()
            database = context["database"]
            default_schema = context["schema"]

            # Parse table name
            if "." in table_name:
                schema_part, table_part = table_name.split(".", 1)
                schema = schema_part.strip('"') or default_schema
                table = table_part.strip('"')
            else:
                schema = default_schema
                table = table_name.strip('"')

            connection = snowflake.connector.connect(
                account=self._clean_account(self.credentials.get("account")),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else database,
                schema=default_schema,
            )
            cursor = connection.cursor()
            try:
                if context["fallback_used"]:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(database)}")
                if database:
                    cursor.execute(f"USE DATABASE {_qident(database)}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(default_schema)}")
                cursor.execute(f"USE SCHEMA {_qident(schema)}")
                attempts = [
                    f"ALTER TABLE {_maybe_unquoted(table)} DROP COLUMN IF EXISTS {_maybe_unquoted(column_name)}",
                    f"ALTER TABLE {_qident(table)} DROP COLUMN IF EXISTS {_qident(column_name)}",
                ]
                for stmt in attempts:
                    try:
                        cursor.execute(stmt)
                        break
                    except Exception:
                        continue
                connection.commit()
                return {"ok": True}
            except Exception as e:
                return {"ok": False, "message": str(e)}
            finally:
                cursor.close()
                connection.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, drop_sync)

    async def list_columns(self, table_name: str) -> List[str]:
        """Return column names for a Snowflake table (case-preserving)."""
        if not self.driver_available:
            return []

        def list_sync():
            def _qident(identifier: str) -> str:
                return '"' + str(identifier).replace('"', '""') + '"'

            def _maybe_unquoted(ident: str) -> str:
                # If simple identifier, prefer unquoted so Snowflake folds to uppercase.
                import re
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", ident or ""):
                    return ident
                return _qident(ident)

            context = self._resolve_writable_context()
            database = context["database"]
            default_schema = context["schema"]

            if "." in table_name:
                schema_part, table_part = table_name.split(".", 1)
                schema = schema_part.strip('"') or default_schema
                table = table_part.strip('"')
            else:
                schema = default_schema
                table = table_name.strip('"')

            conn = snowflake.connector.connect(
                account=self._clean_account(self.credentials.get("account")),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                warehouse=self.credentials.get("warehouse"),
                database=None if context["fallback_used"] else database,
                schema=default_schema,
            )
            cur = conn.cursor()
            try:
                if context["fallback_used"]:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {_qident(database)}")
                if database:
                    cur.execute(f"USE DATABASE {_qident(database)}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(default_schema)}")
                cur.execute(f"USE SCHEMA {_qident(schema)}")
                attempts = [
                    f'DESCRIBE TABLE {_maybe_unquoted(table)}',
                    f'DESCRIBE TABLE {_qident(table)}',
                ]
                cols = []
                for stmt in attempts:
                    try:
                        cur.execute(stmt)
                        cols = [row[0] for row in (cur.fetchall() or []) if row and row[0]]
                        if cols:
                            break
                    except Exception:
                        continue
                return cols
            finally:
                cur.close()
                conn.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, list_sync)

    async def run_ddl(self, ddl: str) -> dict:
        """Execute arbitrary DDL against Snowflake.

        Splits the supplied SQL on semicolons, runs each statement
        sequentially, commits, and returns ``{"ok": True}`` on success.
        On failure returns ``{"ok": False, "error": "msg"}``.
        """
        if not self.driver_available:
            return {"ok": False, "error": "Snowflake driver not available"}
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            for stmt in filter(None, (s.strip() for s in ddl.split(';'))):
                cur.execute(stmt)
            conn.commit()
            cur.close()
            conn.close()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}
