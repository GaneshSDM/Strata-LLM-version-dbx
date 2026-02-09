try:
    import cx_Oracle
except ImportError:
    import oracledb as cx_Oracle
import asyncio
import inspect
import os
import re
import time
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from .base import DatabaseAdapter

class OracleAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        try:
            import cx_Oracle
            self.driver_available = True
        except ImportError:
            try:
                import oracledb as cx_Oracle
                self.driver_available = True
            except ImportError:
                self.driver_available = False

    def _pick_oracle_client_lib_dir(self) -> Optional[str]:
        """
        Return an Oracle Instant Client directory for thick-mode initialization.

        Order:
        1) credentials['oracle_client_lib_dir']
        2) env ORACLE_CLIENT_LIB_DIR
        3) repo-bundled oracle_client/instantclient_*
        """
        configured = (self.credentials.get("oracle_client_lib_dir") or "").strip()
        if configured and Path(configured).exists():
            return configured

        env_dir = (os.environ.get("ORACLE_CLIENT_LIB_DIR") or "").strip()
        if env_dir and Path(env_dir).exists():
            return env_dir

        # Try repo-bundled client (this repo ships oracle_client/instantclient_* at the root).
        try:
            repo_root = Path(__file__).resolve().parents[2]  # .../backend/adapters/oracle.py -> repo root
            base = repo_root / "oracle_client"
            if not base.exists():
                return None

            candidates = [p for p in base.glob("instantclient_*") if p.is_dir()]
            if not candidates:
                return None

            def version_key(p: Path):
                nums = [int(x) for x in re.findall(r"\d+", p.name)]
                return nums if nums else [0]

            best = max(candidates, key=version_key)
            return str(best)
        except Exception:
            return None

    def _ensure_thick_mode_for_sys_role(self, role: str, driver_module=None):
        """
        SYSDBA/SYSOPER requires thick mode; switch the driver if we're in thin mode.
        Accept an optional driver module so we can reuse this for the ORA-12638 fallback.
        """
        role_upper = (role or "").upper()
        if role_upper not in ("SYSDBA", "SYSOPER"):
            return

        driver = driver_module or cx_Oracle
        is_thin_fn = getattr(driver, "is_thin_mode", None)
        init_fn = getattr(driver, "init_oracle_client", None)
        try:
            if callable(is_thin_fn) and is_thin_fn():
                if not callable(init_fn):
                    raise RuntimeError("SYSDBA/SYSOPER requires Oracle Client libraries (thick mode)")
                client_dir = self._pick_oracle_client_lib_dir()
                if client_dir:
                    init_fn(lib_dir=client_dir)
                else:
                    init_fn()
        except Exception as e:
            raise RuntimeError(f"SYSDBA/SYSOPER connections require Oracle Client libraries (thick mode): {e}")

    def _validate_schema_identifier(self, schema: str) -> Optional[str]:
        cleaned = (schema or "").strip()
        if not cleaned:
            return None
        cleaned = cleaned.upper()
        # Oracle unquoted identifiers: letters, digits, _, $, #; must start with a letter.
        if re.fullmatch(r"[A-Z][A-Z0-9_$#]*", cleaned):
            return cleaned
        return None

    def _connect(self):
        def _filter_connect_kwargs(driver, kwargs):
            try:
                sig = inspect.signature(driver.connect)
                if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
                    return kwargs
                allowed = set(sig.parameters.keys())
                return {key: value for key, value in kwargs.items() if key in allowed}
            except (TypeError, ValueError):
                # Fall back to dropping optional encoding args for compatibility.
                kwargs.pop("encoding", None)
                kwargs.pop("nencoding", None)
                return kwargs

        def _clean(value):
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned if cleaned else None
            return value

        host = _clean(self.credentials.get('host')) or 'localhost'
        port = _clean(self.credentials.get('port')) or 1521
        service_name = _clean(self.credentials.get('service_name')) or _clean(self.credentials.get('database'))
        sid = _clean(self.credentials.get('sid'))
        username = _clean(self.credentials.get('username'))
        password = _clean(self.credentials.get('password'))
        role = (_clean(self.credentials.get('role')) or '').upper()

        if not username or not password:
            raise ValueError("Oracle requires username and password")

        # SYSDBA/SYSOPER requires thick mode; try to switch if we are in thin mode.
        self._ensure_thick_mode_for_sys_role(role)

        connect_timeout = _clean(self.credentials.get("connect_timeout")) or 60
        timeout_val = None
        try:
            if connect_timeout is not None:
                timeout_val = int(connect_timeout)
        except (TypeError, ValueError):
            timeout_val = None

        if service_name:
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        elif sid:
            dsn = cx_Oracle.makedsn(host, port, sid=sid)
        else:
            dsn = cx_Oracle.makedsn(host, port, service_name='XE')

        def _apply_call_timeout(conn, timeout_seconds: int | None) -> None:
            if not timeout_seconds:
                return
            try:
                timeout_ms = int(timeout_seconds) * 1000
            except (TypeError, ValueError):
                return
            # cx_Oracle uses callTimeout, oracledb uses call_timeout.
            for attr in ("call_timeout", "callTimeout"):
                if hasattr(conn, attr):
                    try:
                        setattr(conn, attr, timeout_ms)
                        return
                    except Exception:
                        continue

        connect_kwargs = {
            "user": username,
            "password": password,
            "dsn": dsn,
            "encoding": "UTF-8",
            "nencoding": "UTF-8",
        }
        connect_kwargs = _filter_connect_kwargs(cx_Oracle, connect_kwargs)
        if role == 'SYSDBA':
            connect_kwargs["mode"] = cx_Oracle.SYSDBA
        elif role == 'SYSOPER':
            connect_kwargs["mode"] = cx_Oracle.SYSOPER

        retries = int(self.credentials.get("connect_retries", 2))
        delay = float(self.credentials.get("connect_retry_delay", 2))
        attempt = 0
        while True:
            try:
                connection = cx_Oracle.connect(**connect_kwargs)
                _apply_call_timeout(connection, timeout_val)
                break
            except cx_Oracle.DatabaseError as exc:
                error_msg = str(exc)
                if "ORA-12638" in error_msg:
                    try:
                        import oracledb
                    except ImportError:
                        raise
                    thin_dsn = dsn
                    if isinstance(oracledb, type(cx_Oracle)):
                        # Rebuild with oracledb in case DSN classes differ.
                        if service_name:
                            thin_dsn = oracledb.makedsn(host, port, service_name=service_name)
                        elif sid:
                            thin_dsn = oracledb.makedsn(host, port, sid=sid)
                        else:
                            thin_dsn = oracledb.makedsn(host, port, service_name="XE")
                    thin_kwargs = {
                        "user": username,
                        "password": password,
                        "dsn": thin_dsn,
                        "encoding": "UTF-8",
                        "nencoding": "UTF-8",
                    }
                    thin_kwargs = _filter_connect_kwargs(oracledb, thin_kwargs)
                    if role == "SYSDBA":
                        thin_kwargs["mode"] = oracledb.SYSDBA
                    elif role == "SYSOPER":
                        thin_kwargs["mode"] = oracledb.SYSOPER
                    self._ensure_thick_mode_for_sys_role(role, driver_module=oracledb)
                    if self.credentials.get('externalauth'):
                        thin_kwargs['externalauth'] = True
                    connection = oracledb.connect(**thin_kwargs)
                    _apply_call_timeout(connection, timeout_val)
                    break
                attempt += 1
                if attempt > retries:
                    raise
                time.sleep(delay)
        
        # If a specific schema is provided in credentials, set it as the current schema
        schema = _clean(self.credentials.get('schema'))
        if schema:
            schema = self._validate_schema_identifier(schema)
            cursor = None
            try:
                if not schema:
                    raise ValueError("invalid schema identifier")
                cursor = connection.cursor()
                # Oracle does not allow bind variables in DDL (e.g., ALTER SESSION).
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
            except Exception as e:
                # Don't fail the connection if schema is invalid; just report and continue.
                print(f"Warning: could not set CURRENT_SCHEMA to {schema}: {e}")
            finally:
                if cursor:
                    cursor.close()
        
        return connection
    
    def get_connection(self):
        # Return a connection for synchronous operations
        return self._connect()

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def connect_sync():
                connection = self._connect()
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                cursor.close()
                connection.close()
                return result is not None
            
            import asyncio
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, connect_sync)
            
            if result:
                return {"ok": True, "message": "Oracle connection successful"}
            else:
                return {"ok": False, "message": "Oracle connection failed"}
                
        except Exception as e:
            return {"ok": False, "message": str(e)}

    async def introspect_analysis(self) -> Dict[str, Any]:
        # Check if schema filter was passed in credentials
        schema_filter = self.credentials.get('schema_filter')
        if schema_filter:
            return await self.introspect_analysis_with_schema(schema_filter)
        
        # Also check if a default schema was provided in the connection
        default_schema = self.credentials.get('schema')
        if default_schema:
            return await self.introspect_analysis_with_schema(default_schema)
        
        if not self.driver_available:
            return {
                "database_info": {"type": "Oracle", "version": "19c", "schemas": ["HR"]},
                "tables": [{"schema": "HR", "name": "EMPLOYEES", "type": "TABLE"}],
                "columns": [{"schema": "HR", "table": "EMPLOYEES", "name": "EMPLOYEE_ID", "type": "NUMBER", "nullable": False}],
                "constraints": [], "views": [], "procedures": [], "indexes": [],
                "data_profiles": [{"schema": "HR", "table": "EMPLOYEES", "row_count": 300}],
                "driver_unavailable": True,
                "storage_info": {
                    "database_size": {
                        "total_size": 300,
                    },
                    "tables": [
                        {
                            "schema": "HR",
                            "name": "EMPLOYEES",
                            "total_size": 0,
                            "data_length": 0,
                            "index_length": 0
                        }
                    ]
                }
            }
        
        try:
            def introspect_sync():
                connection = self._connect()
                cursor = connection.cursor()
                
                # Get database version
                cursor.execute("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")
                version_row = cursor.fetchone()
                version = version_row[0] if version_row else "Unknown"
                
                # Get schemas (users in Oracle)
                cursor.execute("""
                    SELECT username FROM all_users 
                    WHERE username NOT IN ('SYS', 'SYSTEM', 'ANONYMOUS', 'APEX_PUBLIC_USER')
                    ORDER BY username
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                
                # Get tables
                tables = []
                data_profiles = []
                if schemas:
                    placeholders = ','.join([f"'{schema}'" for schema in schemas])
                    cursor.execute(f"""
                        SELECT owner, table_name, 'TABLE' as table_type
                        FROM all_tables 
                        WHERE owner IN ({placeholders})
                        ORDER BY owner, table_name
                    """)
                    
                    for row in cursor.fetchall():
                        tables.append({
                            "schema": row[0],
                            "name": row[1],
                            "type": row[2]
                        })
                        
                        # Get row count for each table
                        try:
                            count_cursor = connection.cursor()
                            count_cursor.execute(f'SELECT COUNT(*) FROM "{row[0]}"."{row[1]}"')
                            row_count = count_cursor.fetchone()[0]
                            count_cursor.close()
                            
                            data_profiles.append({
                                "schema": row[0],
                                "table": row[1],
                                "row_count": row_count
                            })
                        except:
                            data_profiles.append({
                                "schema": row[0],
                                "table": row[1],
                                "row_count": 0
                            })
                
                # Get columns
                columns = []
                if schemas:
                    placeholders = ','.join([f"'{schema}'" for schema in schemas])
                    cursor.execute(f"""
                        SELECT owner, table_name, column_name, data_type, 
                               nullable, data_default, char_length
                        FROM all_tab_columns 
                        WHERE owner IN ({placeholders})
                        ORDER BY owner, table_name, column_id
                    """)
                    
                    for row in cursor.fetchall():
                        columns.append({
                            "schema": row[0],
                            "table": row[1],
                            "name": row[2],
                            "type": row[3],
                            "nullable": row[4] == 'Y',
                            "default": row[5],
                            "char_length": row[6]
                        })
                
                # Get constraints
                constraints = []
                if schemas:
                    placeholders = ','.join([f"'{schema}'" for schema in schemas])
                    cursor.execute(f"""
                        SELECT owner, table_name, constraint_name, constraint_type
                        FROM all_constraints 
                        WHERE owner IN ({placeholders})
                        ORDER BY owner, table_name, constraint_name
                    """)
                    
                    for row in cursor.fetchall():
                        constraints.append({
                            "schema": row[0],
                            "table": row[1],
                            "name": row[2],
                            "type": row[3]
                        })
                
                # Get views
                views = []
                if schemas:
                    placeholders = ','.join([f"'{schema}'" for schema in schemas])
                    cursor.execute(f"""
                        SELECT owner, view_name
                        FROM all_views 
                        WHERE owner IN ({placeholders})
                        ORDER BY owner, view_name
                    """)
                    
                    for row in cursor.fetchall():
                        views.append({
                            "schema": row[0],
                            "name": row[1],
                            "type": "VIEW"
                        })
                
                # Prepare tables for storage_info
                storage_tables = []
                for table in tables:
                    storage_tables.append({
                        "schema": table.get("schema"),
                        "name": table.get("name"),
                        "total_size": table.get("total_size", 0),
                        "data_length": table.get("data_length", 0),
                        "index_length": table.get("index_length", 0)
                    })
                
                return {
                    "database_info": {"type": "Oracle", "version": version, "schemas": schemas},
                    "tables": tables,
                    "columns": columns,
                    "constraints": constraints,
                    "views": views,
                    "procedures": [],
                    "indexes": [],
                    "data_profiles": data_profiles,
                    "storage_info": {
                        "database_size": {
                            "total_size": sum(profile.get("row_count", 0) for profile in data_profiles),
                        },
                        "tables": storage_tables
                    }
                }
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, introspect_sync)
            return result
            
        except Exception as e:
            return {
                "database_info": {"type": "Oracle", "version": "Error", "schemas": []},
                "tables": [], "columns": [], "constraints": [], "views": [],
                "procedures": [], "indexes": [], "data_profiles": [],
                "storage_info": {
                    "database_size": {
                        "total_size": 0,
                    },
                    "tables": []
                },
                "error": str(e)
            }
    
    async def introspect_analysis_with_schema(self, schema_name: str) -> Dict[str, Any]:
        if schema_name:
            schema_name = schema_name.strip().upper()
        if not self.driver_available:
            return {
                "database_info": {"type": "Oracle", "version": "19c", "schemas": [schema_name]},
                "tables": [{"schema": schema_name, "name": "EMPLOYEES", "type": "TABLE"}],
                "columns": [{"schema": schema_name, "table": "EMPLOYEES", "name": "EMPLOYEE_ID", "type": "NUMBER", "nullable": False}],
                "constraints": [], "views": [], "procedures": [], "indexes": [],
                "data_profiles": [{"schema": schema_name, "table": "EMPLOYEES", "row_count": 300}],
                "driver_unavailable": True,
                "storage_info": {
                    "database_size": {
                        "total_size": 300,
                    },
                    "tables": [
                        {
                            "schema": schema_name,
                            "name": "EMPLOYEES",
                            "total_size": 0,
                            "data_length": 0,
                            "index_length": 0
                        }
                    ]
                }
            }
        
        try:
            def introspect_sync():
                connection = self._connect()
                cursor = connection.cursor()
                
                # Get database version
                cursor.execute("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")
                version_row = cursor.fetchone()
                version = version_row[0] if version_row else "Unknown"
                
                # Only query the specific schema
                schemas = [schema_name]
                
                # Get tables for the specific schema with storage information
                tables = []
                data_profiles = []
                storage_info_dict = {}
                
                # First get storage information for all segments in the schema
                try:
                    cursor.execute(f"""
                        SELECT segment_name, segment_type, bytes
                        FROM dba_segments 
                        WHERE owner = '{schema_name}'
                    """)
                    
                    for row in cursor.fetchall():
                        segment_name, segment_type, bytes_size = row
                        if segment_name not in storage_info_dict:
                            storage_info_dict[segment_name] = {
                                'data_bytes': 0,
                                'index_bytes': 0,
                                'lob_bytes': 0
                            }
                        
                        if segment_type == 'TABLE':
                            storage_info_dict[segment_name]['data_bytes'] += bytes_size
                        elif segment_type == 'INDEX':
                            storage_info_dict[segment_name]['index_bytes'] += bytes_size
                        elif segment_type in ['LOBSEGMENT', 'LOBINDEX']:
                            storage_info_dict[segment_name]['lob_bytes'] += bytes_size
                
                except Exception as storage_error:
                    print(f"Warning: Could not fetch storage info: {storage_error}")
                    # Continue without storage info if query fails
                
                cursor.execute(f"""
                    SELECT table_name
                    FROM dba_tables 
                    WHERE owner = '{schema_name}'
                    ORDER BY table_name
                """)
                
                for row in cursor.fetchall():
                    table_name = row[0]
                    table_storage = storage_info_dict.get(table_name, {'data_bytes': 0, 'index_bytes': 0, 'lob_bytes': 0})
                    
                    tables.append({
                        "schema": schema_name,
                        "name": table_name,
                        "type": "TABLE"
                    })
                    
                    # Get row count for each table
                    try:
                        count_cursor = connection.cursor()
                        count_cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                        row_count = count_cursor.fetchone()[0]
                        count_cursor.close()
                        
                        data_profiles.append({
                            "schema": schema_name,
                            "table": table_name,
                            "row_count": row_count
                        })
                    except:
                        data_profiles.append({
                            "schema": schema_name,
                            "table": table_name,
                            "row_count": 0
                        })
                
                # Get columns for the specific schema
                columns = []
                
                cursor.execute(f"""
                    SELECT table_name, column_name, data_type, 
                           nullable, data_default, char_length
                    FROM dba_tab_columns 
                    WHERE owner = '{schema_name}'
                    ORDER BY table_name, column_id
                """)
                
                for row in cursor.fetchall():
                    columns.append({
                        "schema": schema_name,
                        "table": row[0],
                        "name": row[1],
                        "type": row[2],
                        "nullable": row[3] == 'Y',
                        "default": row[4],
                        "char_length": row[5]
                    })
                
                # Get constraints for the specific schema
                constraints = []
                
                cursor.execute(f"""
                    SELECT table_name, constraint_name, constraint_type
                    FROM dba_constraints 
                    WHERE owner = '{schema_name}'
                    ORDER BY table_name, constraint_name
                """)
                
                for row in cursor.fetchall():
                    constraints.append({
                        "schema": schema_name,
                        "table": row[0],
                        "name": row[1],
                        "type": row[2]
                    })
                
                # Get views for the specific schema
                views = []
                
                cursor.execute(f"""
                    SELECT view_name
                    FROM dba_views 
                    WHERE owner = '{schema_name}'
                    ORDER BY view_name
                """)
                
                for row in cursor.fetchall():
                    views.append({
                        "schema": schema_name,
                        "name": row[0],
                        "type": "VIEW"
                    })
                
                # Prepare tables for storage_info with actual sizes
                storage_tables = []
                total_data_size = 0
                total_index_size = 0
                
                for table in tables:
                    table_name = table["name"]
                    storage_data = storage_info_dict.get(table_name, {'data_bytes': 0, 'index_bytes': 0, 'lob_bytes': 0})
                    
                    data_size = storage_data['data_bytes']
                    index_size = storage_data['index_bytes']
                    lob_size = storage_data['lob_bytes']
                    total_size = data_size + index_size + lob_size
                    
                    total_data_size += data_size
                    total_index_size += index_size
                    
                    storage_tables.append({
                        "schema": schema_name,
                        "name": table_name,
                        "total_size": total_size,
                        "data_length": data_size,
                        "index_length": index_size,
                        "lob_length": lob_size
                    })
                
                return {
                    "database_info": {"type": "Oracle", "version": version, "schemas": schemas},
                    "tables": tables,
                    "columns": columns,
                    "constraints": constraints,
                    "views": views,
                    "procedures": [],
                    "indexes": [],
                    "data_profiles": data_profiles,
                    "storage_info": {
                        "database_size": {
                            "total_size": total_data_size + total_index_size,  # Actual storage size in bytes
                            "data_size": total_data_size,
                            "index_size": total_index_size
                        },
                        "tables": storage_tables
                    }
                }
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, introspect_sync)
            return result
            
        except Exception as e:
            return {
                "database_info": {"type": "Oracle", "version": "Error", "schemas": []},
                "tables": [], "columns": [], "constraints": [], "views": [],
                "procedures": [], "indexes": [], "data_profiles": [],
                "storage_info": {
                    "database_size": {
                        "total_size": 0,
                    },
                    "tables": []
                },
                "error": str(e)
            }

    async def extract_objects(self, selected_tables: List[str] | None = None) -> Dict[str, Any]:
        # Implementation for extracting database objects
        # Normalize selected tables for case-insensitive matching.
        selected_set = set([str(t).strip() for t in (selected_tables or []) if str(t).strip()])
        selected_lower = set([t.lower() for t in selected_set])
        
        def _clean_identifier(identifier: str) -> str:
            # Remove quotes and clean the identifier
            return identifier.replace('"', '').replace('`', '').replace('[', '').replace(']', '').strip()
        
        try:
            connection = self._connect()
            cursor = connection.cursor()
            
            extracted_scripts = {
                "user_types": [],
                "sequences": [],
                "tables": [],
                "indexes": [],
                "views": [],
                "materialized_views": [],
                "triggers": [],
                "procedures": [],
                "functions": [],
                "constraints": [],
                "grants": [],
                "validation_scripts": []
            }
            
            # Extract tables
            if selected_tables:
                # Build query to extract only selected tables
                table_conditions = []
                for table_ref in selected_tables:
                    table_ref_clean = _clean_identifier(table_ref)
                    if '.' in table_ref_clean:
                        schema, table = table_ref_clean.split('.', 1)
                        table_conditions.append(f"(owner = UPPER('{schema}') AND table_name = UPPER('{table}'))")
                    else:
                        table_conditions.append(f"table_name = UPPER('{table_ref_clean}')")
                
                if table_conditions:
                    where_clause = " OR ".join(table_conditions)
                    cursor.execute(f"""
                        SELECT owner, table_name
                        FROM all_tables
                        WHERE {where_clause}
                        ORDER BY owner, table_name
                    """)
            else:
                # Extract all tables
                cursor.execute("""
                    SELECT owner, table_name
                    FROM all_tables
                    ORDER BY owner, table_name
                """)
            
            tables = cursor.fetchall()
            
            for owner, table_name in tables:
                # Try to use DBMS_METADATA.GET_DDL for accurate, complete DDL extraction
                table_ddl = None
                try:
                    ddl_cursor = connection.cursor()

                    # Configure DBMS_METADATA for clean output
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',false); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',true); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS',true); END;")
                    ddl_cursor.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS_AS_ALTER',false); END;")

                    # Get the DDL
                    ddl_cursor.execute(f"""
                        SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}', '{owner}') FROM DUAL
                    """)
                    result = ddl_cursor.fetchone()
                    if result and result[0]:
                        # CLOB data needs to be read properly
                        table_ddl = result[0].read() if hasattr(result[0], 'read') else str(result[0])
                        # Clean up the DDL
                        table_ddl = table_ddl.strip()
                        if not table_ddl.endswith(';'):
                            table_ddl += ';'

                    ddl_cursor.close()
                except Exception as e:
                    # Fallback to manual DDL construction if DBMS_METADATA fails
                    print(f"[ORACLE] DBMS_METADATA failed for {owner}.{table_name}: {e}, using fallback")
                    try:
                        ddl_cursor.close()
                    except:
                        pass

                # Fallback: Manual DDL construction (original logic)
                if not table_ddl:
                    table_ddl = f"CREATE TABLE \"{owner}\".\"{table_name}\" (\n"

                    # Get columns for this table
                    col_cursor = connection.cursor()
                    col_cursor.execute(f"""
                        SELECT column_name, data_type, nullable, data_default, char_length, data_precision, data_scale
                        FROM all_tab_columns
                        WHERE owner = '{owner}' AND table_name = '{table_name}'
                        ORDER BY column_id
                    """)

                    columns = []
                    for col_row in col_cursor.fetchall():
                        col_name, col_type, nullable, default_val, char_len, precision, scale = col_row

                        # Format the column definition
                        col_def = f"    \"{col_name}\" {col_type}"

                        if char_len and 'CHAR' in col_type:
                            col_def += f"({char_len})"
                        elif precision is not None and scale is not None and 'NUMBER' in col_type:
                            col_def += f"({precision}, {scale})"
                        elif precision is not None and 'NUMBER' in col_type:
                            col_def += f"({precision})"

                        if nullable == 'N':
                            col_def += " NOT NULL"

                        if default_val is not None:
                            col_def += f" DEFAULT {default_val}"

                        columns.append(col_def)

                    col_cursor.close()

                    # Get constraints for this table (PK/UK/FK/CHECK)
                    constraint_lines: List[str] = []
                    try:
                        con_cursor = connection.cursor()

                        # Primary key + unique constraints
                        con_cursor.execute(f"""
                            SELECT c.constraint_name,
                                   c.constraint_type,
                                   acc.column_name,
                                   acc.position
                            FROM all_constraints c
                            JOIN all_cons_columns acc
                              ON c.owner = acc.owner
                             AND c.constraint_name = acc.constraint_name
                            WHERE c.owner = '{owner}'
                              AND c.table_name = '{table_name}'
                              AND c.constraint_type IN ('P','U')
                            ORDER BY c.constraint_name, acc.position
                        """)
                        pk_unique_map: Dict[str, Dict[str, Any]] = {}
                        for cname, ctype, col, _pos in con_cursor.fetchall():
                            entry = pk_unique_map.setdefault(cname, {"type": ctype, "cols": []})
                            entry["cols"].append(col)

                        for cname, info in pk_unique_map.items():
                            cols = ", ".join([f"\"{c}\"" for c in info.get("cols", [])])
                            if not cols:
                                continue
                            if info.get("type") == "P":
                                constraint_lines.append(f"    CONSTRAINT \"{cname}\" PRIMARY KEY ({cols})")
                            else:
                                constraint_lines.append(f"    CONSTRAINT \"{cname}\" UNIQUE ({cols})")

                        # Foreign keys
                        con_cursor.execute(f"""
                            SELECT c.constraint_name,
                                   acc.column_name,
                                   rc.owner AS r_owner,
                                   rc.table_name AS r_table,
                                   racc.column_name AS r_column,
                                   acc.position
                            FROM all_constraints c
                            JOIN all_cons_columns acc
                              ON c.owner = acc.owner
                             AND c.constraint_name = acc.constraint_name
                            JOIN all_constraints rc
                              ON c.r_owner = rc.owner
                             AND c.r_constraint_name = rc.constraint_name
                            JOIN all_cons_columns racc
                              ON rc.owner = racc.owner
                             AND rc.constraint_name = racc.constraint_name
                             AND acc.position = racc.position
                            WHERE c.owner = '{owner}'
                              AND c.table_name = '{table_name}'
                              AND c.constraint_type = 'R'
                            ORDER BY c.constraint_name, acc.position
                        """)
                        fk_map: Dict[str, Dict[str, Any]] = {}
                        for cname, col, r_owner, r_table, r_col, _pos in con_cursor.fetchall():
                            entry = fk_map.setdefault(
                                cname,
                                {"cols": [], "r_owner": r_owner, "r_table": r_table, "r_cols": []}
                            )
                            entry["cols"].append(col)
                            entry["r_cols"].append(r_col)

                        for cname, info in fk_map.items():
                            cols = ", ".join([f"\"{c}\"" for c in info.get("cols", [])])
                            r_cols = ", ".join([f"\"{c}\"" for c in info.get("r_cols", [])])
                            if cols and r_cols:
                                constraint_lines.append(
                                    f"    CONSTRAINT \"{cname}\" FOREIGN KEY ({cols}) REFERENCES \"{info['r_owner']}\".\"{info['r_table']}\" ({r_cols})"
                                )

                        # Check constraints (exclude NOT NULL system checks)
                        con_cursor.execute(f"""
                            SELECT constraint_name, search_condition
                            FROM all_constraints
                            WHERE owner = '{owner}'
                              AND table_name = '{table_name}'
                              AND constraint_type = 'C'
                        """)
                        for cname, condition in con_cursor.fetchall():
                            if not condition:
                                continue
                            normalized = str(condition).strip()
                            upper = normalized.upper()
                            if "IS NOT NULL" in upper:
                                continue
                            if cname.upper().startswith("SYS_"):
                                continue
                            constraint_lines.append(
                                f"    CONSTRAINT \"{cname}\" CHECK ({normalized})"
                            )

                        con_cursor.close()
                    except Exception:
                        # Best-effort; skip constraints on any failure
                        try:
                            con_cursor.close()
                        except Exception:
                            pass

                    all_lines = columns + constraint_lines
                    table_ddl += ",\n".join(all_lines)
                    table_ddl += "\n);"

                extracted_scripts["tables"].append({
                    "schema": owner,
                    "name": table_name,
                    "ddl": table_ddl
                })
            
            cursor.close()
            
            return {
                "ddl_scripts": extracted_scripts,
                "extraction_summary": {
                    "user_types": len(extracted_scripts["user_types"]),
                    "sequences": len(extracted_scripts["sequences"]),
                    "tables": len(extracted_scripts["tables"]),
                    "constraints": len(extracted_scripts["constraints"]),
                    "indexes": len(extracted_scripts["indexes"]),
                    "views": len(extracted_scripts["views"]),
                    "materialized_views": len(extracted_scripts["materialized_views"]),
                    "triggers": len(extracted_scripts["triggers"]),
                    "procedures": len(extracted_scripts["procedures"]),
                    "functions": len(extracted_scripts["functions"]),
                    "grants": len(extracted_scripts["grants"]),
                    "validation_scripts": len(extracted_scripts["validation_scripts"])
                },
                "object_count": sum([
                    len(extracted_scripts["user_types"]),
                    len(extracted_scripts["sequences"]),
                    len(extracted_scripts["tables"]),
                    len(extracted_scripts["constraints"]),
                    len(extracted_scripts["indexes"]),
                    len(extracted_scripts["views"]),
                    len(extracted_scripts["materialized_views"]),
                    len(extracted_scripts["triggers"]),
                    len(extracted_scripts["procedures"]),
                    len(extracted_scripts["functions"]),
                    len(extracted_scripts["grants"]),
                    len(extracted_scripts["validation_scripts"])
                ])
            }
        
        except Exception as e:
            return {"error": str(e)}

    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        # Implementation for creating database objects
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "message": "Driver unavailable - simulated creation"}
        
        try:
            connection = self._connect()
            cursor = connection.cursor()
            
            created_objects = []
            errors = []
            
            for obj in translated_ddl:
                target_sql = obj.get("target_sql", "") if obj else ""
                if not target_sql.strip():
                    continue
                
                try:
                    cursor.execute(target_sql)
                    connection.commit()
                    
                    created_objects.append({
                        "name": obj.get("name", "unknown"),
                        "schema": obj.get("schema", "public"),
                        "kind": obj.get("kind", "unknown"),
                        "status": "created"
                    })
                except Exception as e:
                    error_msg = str(e)
                    errors.append({
                        "name": obj.get("name", "unknown"),
                        "schema": obj.get("schema", "public"),
                        "kind": obj.get("kind", "unknown"),
                        "error": error_msg,
                        "sql": target_sql
                    })
                    # Continue with other objects even if one fails
                    continue
            
            cursor.close()
            connection.close()
            
            result = {
                "ok": len(errors) == 0,  # Success if no errors
                "created_count": len(created_objects),
                "total_processed": len(translated_ddl),
                "created_objects": created_objects
            }
            
            if errors:
                result["errors"] = errors
                result["message"] = f"Created {len(created_objects)} objects with {len(errors)} errors"
            else:
                result["message"] = f"Successfully created {len(created_objects)} objects"
            
            return result
        
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "message": f"Failed to create objects: {str(e)}"
            }

    async def copy_table_data(
        self,
        table_name: str,
        source_adapter: 'DatabaseAdapter',
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        # Implementation for copying table data
        if not self.driver_available:
            return {
                "ok": True,
                "driver_unavailable": True,
                "rows_copied": 0,
                "message": "Driver unavailable - simulated data copy"
            }
        
        try:
            # Parse table name to get schema and table
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
            else:
                schema = None
                table = table_name
            
            # Establish connections to both source and target
            target_connection = self._connect()
            target_cursor = target_connection.cursor()
            
            source_connection = source_adapter.get_connection()
            source_cursor = source_connection.cursor()
            
            # Build proper table reference
            quoted_table_name = f'"{table}"'
            if schema:
                quoted_table_name = f'"{schema}".{quoted_table_name}'
            
            # First, get the column information from the target table
            target_cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{table}')")
            target_columns = [row[0] for row in target_cursor.fetchall()]
            requested_columns = [str(c) for c in (columns or []) if str(c or "").strip()]
            if requested_columns:
                target_by_lower = {str(col).lower(): col for col in target_columns}
                filtered = [target_by_lower[c.lower()] for c in requested_columns if c.lower() in target_by_lower]
                if not filtered:
                    return {"ok": False, "rows_copied": 0, "error": "Selected columns not found in target"}
                target_columns = filtered
            columns_str = ', '.join([f'"{col}"' for col in target_columns])
            
            # Query to get data from source
            source_query = f"SELECT {columns_str} FROM {quoted_table_name}"
            
            # Execute source query to get data
            source_cursor.execute(source_query)
            
            rows_copied = 0
            batch = []
            
            # Fetch data in chunks and insert into target
            while True:
                rows = source_cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Prepare insert statement
                placeholders = ', '.join([':' + str(i+1) for i in range(len(target_columns))])
                insert_sql = f"INSERT INTO {quoted_table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Insert batch of rows
                batch_inserted = 0
                for row in rows:
                    try:
                        target_cursor.execute(insert_sql, row)
                        rows_copied += 1
                        batch_inserted += 1
                    except Exception as e:
                        # If individual row fails, log and continue
                        print(f"Row insert failed for {quoted_table_name}: {str(e)}")
                        continue
                
                # Commit periodically to avoid large transactions
                if rows_copied % chunk_size == 0:
                    target_connection.commit()
                if callable(progress_cb):
                    try:
                        progress_cb(rows_copied, batch_inserted)
                    except Exception:
                        pass
            
            # Final commit
            target_connection.commit()
            
            # Close cursors and connections
            source_cursor.close()
            target_cursor.close()
            source_connection.close()
            target_connection.close()
            
            return {
                "ok": True,
                "rows_copied": rows_copied,
                "status": "Success",
                "message": f"Copied {rows_copied} rows to {table_name}"
            }
        
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "rows_copied": 0,
                "status": "Error",
                "message": f"Failed to copy data: {str(e)}",
                "traceback": traceback.format_exc()
            }

    async def get_table_row_count(self, table_name: str) -> int:
        # Get the row count for a specific table
        if not self.driver_available:
            return 0  # Return 0 if driver is unavailable
        
        try:
            connection = self._connect()
            cursor = connection.cursor()
            
            # Parse table name to get schema and table
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                # Quote both schema and table name
                quoted_table_name = f'"{schema}"."{table}"'
            else:
                # Just quote the table name
                quoted_table_name = f'"{table_name}"'
            
            # Execute query to get row count
            cursor.execute(f"SELECT COUNT(*) FROM {quoted_table_name}")
            row_count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            return int(row_count) if row_count is not None else 0
        
        except Exception as e:
            print(f"Could not get row count for {table_name}: {str(e)}")
            # Return 0 if there's an error (e.g., table doesn't exist)
            return 0

    async def run_validation_checks(self, source_adapter: 'DatabaseAdapter') -> Dict[str, Any]:
        # Implementation for running validation checks
        pass

    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        # Implementation for dropping tables
        pass

    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        """
        Rename a column in the Oracle database.
        Uses ALTER TABLE table_name RENAME COLUMN old_name TO new_name syntax.
        """
        if not self.driver_available:
            return {
                "ok": False,
                "message": "Oracle driver not available",
                "driver_unavailable": True
            }
        
        try:
            def rename_sync():
                connection = self._connect()
                cursor = connection.cursor()
                
                # Properly quote identifiers to handle special characters and reserved words
                def _quote_identifier(identifier: str) -> str:
                    # Remove any existing quotes and properly escape
                    clean_id = identifier.replace('"', '').strip()
                    return f'"{clean_id}"'
                
                quoted_table = _quote_identifier(table_name)
                quoted_old_col = _quote_identifier(old_column_name)
                quoted_new_col = _quote_identifier(new_column_name)
                
                # Build the ALTER TABLE statement
                alter_sql = f"ALTER TABLE {quoted_table} RENAME COLUMN {quoted_old_col} TO {quoted_new_col}"
                
                try:
                    cursor.execute(alter_sql)
                    connection.commit()
                    result = {
                        "ok": True,
                        "message": f"Successfully renamed column {old_column_name} to {new_column_name} in {table_name}",
                        "sql_executed": alter_sql
                    }
                except Exception as e:
                    connection.rollback()
                    error_msg = str(e)
                    result = {
                        "ok": False,
                        "message": f"Failed to rename column: {error_msg}",
                        "sql_executed": alter_sql,
                        "error": error_msg
                    }
                finally:
                    cursor.close()
                    connection.close()
                
                return result
            
            import asyncio
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, rename_sync)
            return result
        
        except Exception as e:
            return {
                "ok": False,
                "message": f"Error during column rename operation: {str(e)}",
                "error": str(e)
            }

def get_adapter(credentials: dict):
    return OracleAdapter(credentials)
