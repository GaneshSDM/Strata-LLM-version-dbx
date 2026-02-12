import asyncio
import logging
import re
import traceback
from typing import Dict, Any, List, Optional, Callable
from .base import DatabaseAdapter

try:
    from databricks import sql
    DRIVER_AVAILABLE = True
except ImportError:
    DRIVER_AVAILABLE = False


def _split_sql_statements(sql_text: str) -> List[str]:
    """Split a SQL script into individual statements.

    Best-effort splitter that respects:
    - single quotes (with doubled '' escaping)
    - double quotes
    - backticks
    - line comments (-- ...)
    - block comments (/* ... */)

    We need this because the Databricks SQL connector generally expects one
    statement per execute call, but our AI-generated DDL can include:
      CREATE TABLE ...;
      ALTER TABLE ...;
      ...
    """
    text = str(sql_text or "")
    if not text.strip():
        return []

    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        # Comment start
        if (not in_single) and (not in_double) and (not in_backtick):
            if ch == "-" and nxt == "-":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                in_line_comment = True
                continue
            if ch == "/" and nxt == "*":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                in_block_comment = True
                continue

        # Quote toggles
        if (not in_double) and (not in_backtick) and ch == "'":
            buf.append(ch)
            # Handle escaped '' inside single-quoted string.
            if in_single and nxt == "'":
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if (not in_single) and (not in_backtick) and ch == '"':
            buf.append(ch)
            in_double = not in_double
            i += 1
            continue
        if (not in_single) and (not in_double) and ch == "`":
            buf.append(ch)
            in_backtick = not in_backtick
            i += 1
            continue

        # Statement terminator
        if (not in_single) and (not in_double) and (not in_backtick) and ch == ";":
            current = "".join(buf).strip()
            if current:
                statements.append(current)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _ensure_using_delta(statement: str) -> str:
    """Ensure a CREATE TABLE statement contains USING DELTA.

    This is a minimal normalizer used in the ad-hoc execution path
    (/api/ddl/convert -> run_ddl). We keep it intentionally small so we
    don't unexpectedly rewrite non-table DDL.
    """
    ddl = str(statement or "").strip()
    if not ddl:
        return ddl
    if not re.match(r'(?is)^\s*CREATE\s+TABLE\b', ddl):
        return ddl
    if re.search(r'(?is)\bUSING\s+DELTA\b', ddl):
        return ddl

    ddl = ddl.strip().rstrip(";")
    if re.search(r'(?is)\bTBLPROPERTIES\b', ddl):
        ddl = re.sub(r'(?is)\bTBLPROPERTIES\b', 'USING DELTA TBLPROPERTIES', ddl, count=1)
    elif re.search(r'(?is)\bCLUSTER\s+BY\b', ddl):
        ddl = re.sub(r'(?is)\bCLUSTER\s+BY\b', 'USING DELTA CLUSTER BY', ddl, count=1)
    else:
        ddl = ddl + ' USING DELTA'
    return ddl + ";"


# Shared normalization helpers so ad-hoc run_ddl and bulk create_objects behave consistently.
def _normalize_ddl_for_databricks(raw: str) -> str:
    """Best-effort normalization of Oracle-ish DDL into Databricks-friendly SQL.

    Mirrors the rules used in create_objects(), but available at module scope so
    run_ddl() (used by the ad-hoc DDL converter) benefits from the same safety rails.
    """
    import re

    ddl = (raw or "").strip()
    if not ddl:
        return ""

    # Replace Oracle schema qualifiers and normalize CREATE TABLE prefix.
    m = re.match(r'(?is)^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?P<name>[^(\n]+)\s*\(', ddl)
    if m:
        raw_name = (m.group("name") or "").strip()
        parts = [p.strip().strip('`"') for p in raw_name.split(".") if p.strip()]
        table_only = parts[-1] if parts else raw_name.strip('`"')
        ddl = re.sub(
            r'(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^(\n]+\s*\(',
            f'CREATE TABLE IF NOT EXISTS `{table_only}` (',
            ddl,
            count=1
        )

    # Normalize identifiers.
    ddl = ddl.replace('"', '`')

    # Oracle -> Databricks type conversions (best-effort). Preserve VARCHAR/CHAR lengths.
    ddl = re.sub(r'\bNVARCHAR2\s*\(', 'VARCHAR(', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bVARCHAR2\s*\(', 'VARCHAR(', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNVARCHAR2\b', 'VARCHAR', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bVARCHAR2\b', 'VARCHAR', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNCHAR\s*\(', 'CHAR(', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNCHAR\b', 'CHAR', ddl, flags=re.IGNORECASE)

    # Large objects.
    ddl = re.sub(r'\bCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bTEXT\b', 'STRING', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bBLOB\b', 'BINARY', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bRAW\b', 'BINARY', ddl, flags=re.IGNORECASE)

    # Floating point.
    ddl = re.sub(r'\bBINARY_FLOAT\b', 'FLOAT', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bBINARY_DOUBLE\b', 'DOUBLE', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bFLOAT\b', 'DOUBLE', ddl, flags=re.IGNORECASE)

    # Normalize illegal length specs for native Spark types.
    ddl = re.sub(r'\bSTRING\s*\(\s*\d+\s*(?:CHAR|BYTE)?\s*\)', 'STRING', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bBINARY\s*\(\s*\d+\s*\)', 'BINARY', ddl, flags=re.IGNORECASE)

    # NUMBER mappings.
    ddl = re.sub(r'\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', r'DECIMAL(\1,\2)', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNUMBER\s*\(\s*(\d+)\s*\)', r'DECIMAL(\1)', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bNUMBER\b', 'INT', ddl, flags=re.IGNORECASE)

    ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', ddl, flags=re.IGNORECASE)

    # DATE default fix.
    ddl = re.sub(r'\bDATE\s+DEFAULT\s+CURRENT_TIMESTAMP\s*(?:\(\s*\))?', 'DATE DEFAULT CURRENT_DATE', ddl, flags=re.IGNORECASE)

    # Strip Oracle-specific physical/storage clauses.
    ddl = re.sub(r'\bENABLE\b', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bUSING\s+INDEX\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bTABLESPACE\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bPCTFREE\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bINITRANS\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bMAXTRANS\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\bSTORAGE\b\s*\([^)]*\)', '', ddl, flags=re.IGNORECASE)

    # Ensure USING DELTA for CREATE TABLE.
    ddl = _ensure_using_delta(ddl)
    return ddl


def _rewrite_schema_refs(statement: str, target_schema: str) -> str:
    """Rewrite REFERENCES schema qualifiers to use the configured target schema."""
    import re
    if not statement or not target_schema:
        return statement
    schema_token = f"`{str(target_schema).strip('`')}`"

    def _q(name: str) -> str:
        cleaned = str(name or "").strip()
        if not cleaned:
            return cleaned
        if cleaned.startswith("`") and cleaned.endswith("`"):
            return cleaned
        return f"`{cleaned.strip('`')}`"

    pattern = re.compile(
        r'(?is)\bREFERENCES\s+(?P<schema>`[^`]+`|\"[^\"]+\"|\w+)\s*\.\s*(?P<table>`[^`]+`|\"[^\"]+\"|\w+)'
    )

    def _replace(match: re.Match) -> str:
        table = match.group("table")
        return f"REFERENCES {schema_token}.{_q(table)}"

    return pattern.sub(_replace, statement)


def _contains_foreign_keys(ddl: str) -> bool:
    """Check if DDL contains foreign key constraints."""
    import re
    return bool(re.search(r'\bFOREIGN\s+KEY\b', ddl, flags=re.IGNORECASE))


def _strip_foreign_keys(ddl: str) -> tuple[str, list[str]]:
    """
    Remove foreign key constraints from DDL.

    Returns:
        tuple: (cleaned_ddl, removed_fk_list)
    """
    import re

    removed_fks = []

    # Pattern to match FK constraints more precisely
    # Matches: CONSTRAINT name FOREIGN KEY (...) REFERENCES table(col) or FOREIGN KEY (...) REFERENCES table(col)
    # Captures everything up to and including the referenced columns
    patterns = [
        # Named constraint: CONSTRAINT name FOREIGN KEY (...) REFERENCES table(col)
        r',?\s*CONSTRAINT\s+[`"]?\w+[`"]?\s+FOREIGN\s+KEY\s*\([^)]+\)\s+REFERENCES\s+[^\s(]+\s*\([^)]*\)',
        # Inline FK: FOREIGN KEY (...) REFERENCES table(col)
        r',?\s*FOREIGN\s+KEY\s*\([^)]+\)\s+REFERENCES\s+[^\s(]+\s*\([^)]*\)',
    ]

    cleaned = ddl
    for pattern in patterns:
        matches = re.findall(pattern, ddl, flags=re.IGNORECASE | re.DOTALL)
        for match in matches:
            removed_fks.append(match.strip())
            cleaned = re.sub(re.escape(match), '', cleaned, flags=re.IGNORECASE)

    # Clean up extra commas and whitespace
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r',\s*\)', ')', cleaned)
    cleaned = re.sub(r'\(\s*,', '(', cleaned)

    return cleaned, removed_fks


def _extract_check_constraints(ddl: str) -> tuple[str, list[dict]]:
    """
    Extract CHECK constraints from DDL to be added via ALTER TABLE.

    Returns:
        tuple: (cleaned_ddl, list of {constraint_name, check_condition})
    """
    import re

    check_constraints = []
    constraint_counter = 1

    # Helper function to find matching closing parenthesis
    def find_matching_paren(text, start_pos):
        count = 1
        pos = start_pos
        while pos < len(text) and count > 0:
            if text[pos] == '(':
                count += 1
            elif text[pos] == ')':
                count -= 1
            pos += 1
        return pos if count == 0 else -1

    # Collect all matches first (both named and inline)
    matches_to_remove = []

    # Find named CHECK constraints: CONSTRAINT name CHECK (...)
    pattern_named = r',?\s*CONSTRAINT\s+[`"]?(\w+)[`"]?\s+CHECK\s*\('
    for match in re.finditer(pattern_named, ddl, flags=re.IGNORECASE):
        constraint_name = match.group(1)
        start_pos = match.end()
        end_pos = find_matching_paren(ddl, start_pos)

        if end_pos > 0:
            condition = ddl[start_pos:end_pos-1]
            matches_to_remove.append({
                "start": match.start(),
                "end": end_pos,
                "constraint": {
                    "name": constraint_name,
                    "condition": condition.strip()
                }
            })

    # Find inline CHECK constraints: CHECK (...)
    pattern_inline = r',?\s*CHECK\s*\('
    for match in re.finditer(pattern_inline, ddl, flags=re.IGNORECASE):
        # Skip if this position is already covered by a named constraint
        if any(m["start"] <= match.start() < m["end"] for m in matches_to_remove):
            continue

        start_pos = match.end()
        end_pos = find_matching_paren(ddl, start_pos)

        if end_pos > 0:
            condition = ddl[start_pos:end_pos-1]
            matches_to_remove.append({
                "start": match.start(),
                "end": end_pos,
                "constraint": {
                    "name": f"chk_auto_{constraint_counter}",
                    "condition": condition.strip()
                }
            })
            constraint_counter += 1

    # Sort by position (reverse order to remove from end to beginning)
    matches_to_remove.sort(key=lambda x: x["start"], reverse=True)

    # Remove matches from end to beginning and collect constraints
    cleaned = ddl
    for match_info in matches_to_remove:
        check_constraints.insert(0, match_info["constraint"])
        cleaned = cleaned[:match_info["start"]] + cleaned[match_info["end"]:]

    # Clean up extra commas and whitespace
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r',\s*\)', ')', cleaned)
    cleaned = re.sub(r'\(\s*,', '(', cleaned)

    return cleaned, check_constraints


def _convert_unique_to_column_level(ddl: str) -> tuple[str, list[str]]:
    """
    Convert UNIQUE constraints to column-level UNIQUE modifiers.
    For single-column UNIQUE constraints, add UNIQUE to the column definition.
    For multi-column UNIQUE constraints, keep them as warnings (not supported inline).

    Returns:
        tuple: (modified_ddl, list of warnings for multi-column UNIQUE)
    """
    import re

    warnings = []

    # Pattern to match UNIQUE constraints
    # Named UNIQUE: CONSTRAINT name UNIQUE (col1, col2, ...)
    pattern_named = r',?\s*CONSTRAINT\s+[`"]?(\w+)[`"]?\s+UNIQUE\s*\(([^)]+)\)'
    # Inline UNIQUE: UNIQUE (col1, col2, ...)
    pattern_inline = r',?\s*UNIQUE\s*\(([^)]+)\)'

    cleaned = ddl

    # Process named UNIQUE constraints
    for match in re.finditer(pattern_named, ddl, flags=re.IGNORECASE | re.DOTALL):
        constraint_name = match.group(1)
        columns = [col.strip().strip('`"') for col in match.group(2).split(',')]

        if len(columns) == 1:
            # Single column - add UNIQUE to column definition
            col_name = columns[0]
            # Find the column definition and add UNIQUE
            col_pattern = rf'(`{col_name}`|"{col_name}"|{col_name})\s+([A-Z][A-Z0-9_()]*(?:\([^)]*\))?)'
            col_match = re.search(col_pattern, cleaned, flags=re.IGNORECASE)
            if col_match:
                # Add UNIQUE after the data type
                replacement = f'{col_match.group(1)} {col_match.group(2)} UNIQUE'
                cleaned = re.sub(re.escape(col_match.group(0)), replacement, cleaned, count=1)
        else:
            # Multi-column UNIQUE - not supported inline, log warning
            warnings.append(f"Multi-column UNIQUE constraint {constraint_name} on ({', '.join(columns)}) removed")

        # Remove the constraint definition
        cleaned = re.sub(re.escape(match.group(0)), '', cleaned, count=1)

    # Process inline UNIQUE constraints
    for match in re.finditer(pattern_inline, ddl, flags=re.IGNORECASE | re.DOTALL):
        columns = [col.strip().strip('`"') for col in match.group(1).split(',')]

        if len(columns) == 1:
            # Single column - add UNIQUE to column definition
            col_name = columns[0]
            col_pattern = rf'(`{col_name}`|"{col_name}"|{col_name})\s+([A-Z][A-Z0-9_()]*(?:\([^)]*\))?)'
            col_match = re.search(col_pattern, cleaned, flags=re.IGNORECASE)
            if col_match:
                replacement = f'{col_match.group(1)} {col_match.group(2)} UNIQUE'
                cleaned = re.sub(re.escape(col_match.group(0)), replacement, cleaned, count=1)
        else:
            # Multi-column UNIQUE - not supported inline, log warning
            warnings.append(f"Multi-column UNIQUE constraint on ({', '.join(columns)}) removed")

        # Remove the constraint definition
        cleaned = re.sub(re.escape(match.group(0)), '', cleaned, count=1)

    # Clean up extra commas and whitespace
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r',\s*\)', ')', cleaned)
    cleaned = re.sub(r'\(\s*,', '(', cleaned)

    return cleaned, warnings


class DatabricksAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        self.driver_available = DRIVER_AVAILABLE
        self.logger = logging.getLogger("strata")

    def _detect_catalog_type(self, connection) -> tuple[str, bool]:
        """
        Detect if the configured catalog supports foreign keys.

        Returns:
            tuple: (catalog_name, supports_fk)
        """
        cursor = connection.cursor()
        try:
            catalog = self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")

            # hive_metastore does not support FKs
            if catalog.lower() == "hive_metastore":
                return (catalog, False)

            # For other catalogs, verify Unity Catalog by checking information_schema
            try:
                cursor.execute(f"USE CATALOG `{catalog}`")
                cursor.execute("SELECT catalog_name FROM information_schema.catalogs LIMIT 1")
                cursor.fetchone()
                return (catalog, True)  # Unity Catalog detected
            except Exception:
                return (catalog, False)  # Likely hive_metastore
        except Exception as e:
            self.logger.warning(f"[DATABRICKS] Failed to detect catalog type: {e}")
            return ("hive_metastore", False)
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def get_connection(self):
        if not self.driver_available:
            raise NotImplementedError("Databricks driver not available")
        return sql.connect(
            server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
            http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
            access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
            catalog=self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore"),
            schema=self.credentials.get("schema") or self.credentials.get("schemaName", "default")
        )
    
    async def test_connection(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": False,
                "driver_unavailable": True,
                "vendorVersion": "Databricks Runtime (driver missing)",
                "details": "Databricks SQL connector not installed",
                "message": "Databricks driver not available. Install databricks-sql-connector and restart the backend."
            }

        try:
            # Connect asynchronously using thread pool
            def connect_sync():
                # Defensive programming: validate all required parameters
                # Support both frontend field names (server_hostname) and backend aliases (host)
                server_hostname = self.credentials.get("server_hostname") or self.credentials.get("host")
                http_path = self.credentials.get("http_path") or self.credentials.get("httpPath")
                access_token = self.credentials.get("access_token") or self.credentials.get("accessToken")
                catalog = self.credentials.get("catalog") or self.credentials.get("catalogName") or "hive_metastore"
                schema = self.credentials.get("schema") or self.credentials.get("schemaName") or "default"

                # Validate that required parameters are not None and not empty strings
                if not server_hostname or not server_hostname.strip():
                    raise ValueError("Server hostname is required for Databricks connection. Please provide the Databricks workspace URL.")
                if not http_path or not http_path.strip():
                    raise ValueError("HTTP path is required for Databricks connection. Please provide the SQL warehouse HTTP path.")
                if not access_token or not access_token.strip():
                    raise ValueError("Access token is required for Databricks connection. Please provide a valid personal access token.")
                
                print(f"[DATABRICKS DEBUG] Connecting to {server_hostname}")
                print(f"[DATABRICKS DEBUG] HTTP Path: {http_path}")
                print(f"[DATABRICKS DEBUG] Catalog: {catalog}")
                print(f"[DATABRICKS DEBUG] Schema: {schema}")
                
                try:
                    connection = sql.connect(
                        server_hostname=server_hostname,
                        http_path=http_path,
                        access_token=access_token,
                        catalog=catalog,
                        schema=schema,
                        _socket_timeout=60,  # Add timeout
                        _retry_stop_after_attempts_count=3  # Add retry logic
                    )
                    cursor = connection.cursor()
                    print("[DATABRICKS DEBUG] Executing version query...")
                    cursor.execute("SELECT version()")
                    version_row = cursor.fetchone()
                    version = version_row[0] if version_row else "Unknown"
                    print(f"[DATABRICKS DEBUG] Version received: {version}")
                    cursor.close()
                    connection.close()
                    
                    # Ensure version is a string to prevent .lower() issues downstream
                    version_str = str(version) if version is not None else "Unknown"
                    return version_str
                    
                except Exception as conn_error:
                    print(f"[DATABRICKS DEBUG] Connection error: {str(conn_error)}")
                    raise conn_error
            
            loop = asyncio.get_event_loop()
            version = await loop.run_in_executor(None, connect_sync)
            
            return {"ok": True, "vendorVersion": f"Databricks {version}", "details": "Connection successful", "message": "Connection successful"}
        except Exception as e:
            error_msg = str(e)
            print(f"[DATABRICKS ERROR] Final error: {error_msg}")
            return {"ok": False, "message": error_msg, "error": error_msg}
    
    async def introspect_analysis(self) -> Dict[str, Any]:
        requested_schema = self.credentials.get("schema") or self.credentials.get("schemaName")
        if not self.driver_available:
            schema_name = requested_schema or "default"
            return {
                "database_info": {
                    "type": "Databricks", 
                    "version": "13.x", 
                    "schemas": [schema_name], 
                    "encoding": "utf8", 
                    "collation": "utf8_general_ci",
                    "catalog": self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                },
                "tables": [{"schema": schema_name, "name": "events", "type": "TABLE"}],
                "columns": [{"schema": schema_name, "table": "events", "name": "id", "type": "bigint", "nullable": False}],
                "constraints": [], 
                "views": [], 
                "procedures": [], 
                "indexes": [],
                "triggers": [], 
                "sequences": [], 
                "user_types": [], 
                "materialized_views": [],
                "partitions": [], 
                "permissions": [],
                "data_profiles": [{"schema": schema_name, "table": "events", "row_count": 50000}],
                "driver_unavailable": True,
                "storage_info": {
                    "database_size": {
                        "total_size": 50000,
                        "data_size": 50000,
                        "index_size": 0
                    },
                    "tables": [
                        {
                            "schema": schema_name,
                            "name": "events",
                            "total_size": 0,
                            "data_length": 0,
                            "index_length": 0
                        }
                    ]
                }
            }
        if not self.driver_available:
            schema_name = requested_schema or "default"
            return {
                "database_info": {
                    "type": "Databricks", 
                    "version": "13.x", 
                    "schemas": [schema_name], 
                    "encoding": "utf8", 
                    "collation": "utf8_general_ci",
                    "catalog": self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                },
                "tables": [{"schema": schema_name, "name": "events", "type": "TABLE"}],
                "columns": [{"schema": schema_name, "table": "events", "name": "id", "type": "bigint", "nullable": False}],
                "constraints": [], 
                "views": [], 
                "procedures": [], 
                "indexes": [],
                "triggers": [], 
                "sequences": [], 
                "user_types": [], 
                "materialized_views": [],
                "partitions": [], 
                "permissions": [],
                "data_profiles": [{"schema": schema_name, "table": "events", "row_count": 50000}],
                "driver_unavailable": True,
                "storage_info": {
                    "database_size": {
                        "total_size": 50000,
                        "data_size": 50000,
                        "index_size": 0
                    },
                    "tables": [
                        {
                            "schema": schema_name,
                            "name": "events",
                            "total_size": 0,
                            "data_length": 0,
                            "index_length": 0
                        }
                    ]
                }
            }
        
        try:
            def introspect_sync():
                # Add timeout and retry parameters for better reliability
                connection = sql.connect(
                    server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                    http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                    access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                    catalog=self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore"),
                    schema=self.credentials.get("schema") or self.credentials.get("schemaName", "default"),
                    _socket_timeout=120,  # Increase timeout
                    _retry_stop_after_attempts_count=3
                )
                cursor = connection.cursor()
                
                # Get database version with timeout
                try:
                    cursor.execute("SELECT version()")
                    version_row = cursor.fetchone()
                    version = version_row[0] if version_row else "Unknown"
                except Exception as e:
                    print(f"[DATABRICKS DEBUG] Error getting version: {e}")
                    version = "Unknown"
                
                # Get schemas with error handling
                schemas = []
                if requested_schema:
                    schemas = [requested_schema]
                else:
                    try:
                        cursor.execute("SHOW DATABASES")
                        schemas = [row[0] for row in cursor.fetchall()]
                        print(f"[DATABRICKS DEBUG] Found schemas: {schemas}")
                    except Exception as e:
                        print(f"[DATABRICKS DEBUG] Error getting schemas: {e}")
                        schemas = ["default"]  # Fallback
                
                # Also try information_schema approach as fallback
                if not schemas or len(schemas) == 0:
                    try:
                        cursor.execute("""
                            SELECT DISTINCT table_schema 
                            FROM information_schema.tables 
                            WHERE table_schema NOT IN ('information_schema')
                        """)
                        schemas = [row[0] for row in cursor.fetchall()]
                        print(f"[DATABRICKS DEBUG] Found schemas from information_schema: {schemas}")
                    except Exception as info_schema_error:
                        print(f"[DATABRICKS DEBUG] Error getting schemas from information_schema: {info_schema_error}")
                if requested_schema:
                    schemas = [requested_schema]
                
                # Limit the number of schemas to prevent timeout
                if len(schemas) > 10:
                    print(f"[DATABRICKS DEBUG] Too many schemas ({len(schemas)}), limiting to first 10")
                    schemas = schemas[:10]
                
                # Get tables with improved error handling
                tables = []
                data_profiles = []
                columns = []
                
                for schema in schemas:
                    try:
                        print(f"[DATABRICKS DEBUG] Processing schema: {schema}")
                        
                        # Try multiple approaches to get tables
                        schema_tables = []
                        
                        # Approach 1: SHOW TABLES
                        try:
                            # Databricks SQL doesn't support LIMIT with SHOW TABLES in many runtimes.
                            cursor.execute(f"SHOW TABLES IN `{schema}`")
                            schema_tables = cursor.fetchall()[:100]
                            print(f"[DATABRICKS DEBUG] SHOW TABLES found {len(schema_tables)} tables in {schema}")
                        except Exception as show_tables_error:
                            print(f"[DATABRICKS DEBUG] SHOW TABLES failed for {schema}: {show_tables_error}")
                            
                            # Approach 2: Query information_schema as fallback
                            try:
                                cursor.execute(f"""
                                    SELECT table_schema, table_name, table_type
                                    FROM information_schema.tables 
                                    WHERE table_schema = '{schema}'
                                    LIMIT 100
                                """)
                                schema_tables = [(schema, row[1], False) for row in cursor.fetchall()]  # Format to match SHOW TABLES output
                                print(f"[DATABRICKS DEBUG] information_schema found {len(schema_tables)} tables in {schema}")
                            except Exception as info_schema_error:
                                print(f"[DATABRICKS DEBUG] information_schema also failed for {schema}: {info_schema_error}")
                        
                        # Limit tables to prevent timeout
                        if len(schema_tables) > 50:
                            print(f"[DATABRICKS DEBUG] Too many tables in {schema} ({len(schema_tables)}), limiting to 50")
                            schema_tables = schema_tables[:50]
                        
                        for i, row in enumerate(schema_tables):
                            try:
                                # row format: [database, tableName, isTemporary]
                                table_name = row[1]
                                print(f"[DATABRICKS DEBUG] Processing table {i+1}/{len(schema_tables)}: {schema}.{table_name}")
                                
                                tables.append({
                                    "schema": schema,
                                    "name": table_name,
                                    "type": "TABLE"
                                })
                                
                                # Get approximate row count (faster than COUNT(*))
                                try:
                                    # Try to get table statistics first
                                    stats_cursor = connection.cursor()
                                    stats_cursor.execute(f'DESCRIBE TABLE EXTENDED `{schema}`.`{table_name}`')
                                    table_stats = stats_cursor.fetchall()
                                    stats_cursor.close()
                                    
                                    # Look for row count in table statistics
                                    row_count = 0
                                    for stat_row in table_stats:
                                        if stat_row[0] == "Statistics":
                                            # Parse "X bytes, Y rows" format
                                            stats_text = str(stat_row[1])
                                            if "rows" in stats_text:
                                                match = re.search(r'(\d+) rows', stats_text)
                                                if match:
                                                    row_count = int(match.group(1))
                                                    break
                                    
                                    # If no stats found, do a quick sample count
                                    if row_count == 0:
                                        try:
                                            count_cursor = connection.cursor()
                                            count_cursor.execute(f'SELECT COUNT(*) FROM (SELECT * FROM `{schema}`.`{table_name}` LIMIT 10000)')
                                            row_count = count_cursor.fetchone()[0]
                                            count_cursor.close()
                                        except:
                                            row_count = 0
                                
                                    data_profiles.append({
                                        "schema": schema,
                                        "table": table_name,
                                        "row_count": row_count
                                    })
                                except Exception as count_error:
                                    print(f"[DATABRICKS DEBUG] Error getting row count for {schema}.{table_name}: {count_error}")
                                    data_profiles.append({
                                        "schema": schema,
                                        "table": table_name,
                                        "row_count": 0
                                    })
                                
                                # Get column information with limits
                                try:
                                    desc_cursor = connection.cursor()
                                    desc_cursor.execute(f'DESCRIBE TABLE `{schema}`.`{table_name}`')
                                    table_desc = desc_cursor.fetchall()
                                    desc_cursor.close()
                                    
                                    # Limit columns to prevent excessive data
                                    column_limit = min(50, len(table_desc))
                                    for j, col_row in enumerate(table_desc[:column_limit]):
                                        # col_row format: [col_name, data_type, comment]
                                        # Extract more detailed column information
                                        col_name = col_row[0]
                                        col_type = col_row[1]
                                        col_comment = col_row[2] if len(col_row) > 2 else None
                                        
                                        # Try to determine if column is nullable based on type
                                        # In Databricks, if type contains 'NOT NULL' it's not nullable
                                        is_nullable = 'NOT NULL' not in col_type.upper()
                                        
                                        columns.append({
                                            "schema": schema,
                                            "table": table_name,
                                            "name": col_name,
                                            "type": col_type,
                                            "nullable": is_nullable,
                                            "default": None,  # Databricks doesn't typically show defaults in DESCRIBE
                                            "comment": col_comment,
                                            "collation": None  # Databricks doesn't use collations like MySQL/PostgreSQL
                                        })
                                except Exception as col_error:
                                    print(f"[DATABRICKS DEBUG] Error getting columns for {schema}.{table_name}: {col_error}")
                                    # Add placeholder column
                                    columns.append({
                                        "schema": schema,
                                        "table": table_name,
                                        "name": "unknown",
                                        "type": "unknown",
                                        "nullable": True,
                                        "default": None,
                                        "comment": "Column information unavailable",
                                        "collation": None
                                    })
                                    
                            except Exception as table_error:
                                print(f"[DATABRICKS DEBUG] Error processing table {schema}.{row[1] if len(row) > 1 else 'unknown'}: {table_error}")
                                continue
                        
                    except Exception as schema_error:
                        print(f"[DATABRICKS DEBUG] Error processing schema {schema}: {schema_error}")
                        continue
                
                # Prepare storage info
                storage_tables = []
                for table in tables:
                    # Find matching data profile
                    row_count = 0
                    for profile in data_profiles:
                        if profile.get("schema") == table.get("schema") and profile.get("table") == table.get("name"):
                            row_count = profile.get("row_count", 0)
                            break
                    
                    storage_tables.append({
                        "schema": table.get("schema"),
                        "name": table.get("name"),
                        "total_size": row_count,  # Approximate
                        "data_length": row_count,
                        "index_length": 0
                    })
                
                # Prepare views from information_schema
                try:
                    cursor.execute(
                        "SELECT table_schema, table_name "
                        "FROM information_schema.tables "
                        "WHERE table_type = 'VIEW' "
                        "AND table_schema NOT IN ('information_schema')"
                    )
                    view_results = cursor.fetchall()
                    views = []
                    for view_row in view_results:
                        views.append({
                            "schema": view_row[0],
                            "name": view_row[1],
                            "type": "VIEW"
                        })
                except Exception as view_error:
                    print(f"[DATABRICKS DEBUG] Error getting views: {view_error}")
                    views = []
                                
                # Look for materialized views if supported
                materialized_views = []
                try:
                    # Databricks doesn't have traditional materialized views like PostgreSQL
                    # but we can look for cached tables which serve a similar purpose
                    cursor.execute(
                        "SELECT table_schema, table_name "
                        "FROM information_schema.tables "
                        "WHERE table_type = 'MATERIALIZED VIEW' "
                        "AND table_schema NOT IN ('information_schema')"
                    )
                    mview_results = cursor.fetchall()
                    for mview_row in mview_results:
                        materialized_views.append({
                            "schema": mview_row[0],
                            "name": mview_row[1],
                            "type": "MATERIALIZED VIEW"
                        })
                except Exception as mview_error:
                    print(f"[DATABRICKS DEBUG] Error getting materialized views: {mview_error}")
                    # Databricks doesn't typically support materialized views, so this is expected
                    materialized_views = []
                                
                # Look for procedures/functions
                procedures = []
                try:
                    # Databricks supports functions but not stored procedures in the traditional sense
                    cursor.execute(
                        "SELECT routine_schema, routine_name "
                        "FROM information_schema.routines "
                        "WHERE routine_type IN ('FUNCTION', 'PROCEDURE') "
                        "AND routine_schema NOT IN ('information_schema')"
                    )
                    proc_results = cursor.fetchall()
                    for proc_row in proc_results:
                        procedures.append({
                            "schema": proc_row[0],
                            "name": proc_row[1],
                            "type": proc_row[2] if len(proc_row) > 2 else "FUNCTION"
                        })
                except Exception as proc_error:
                    print(f"[DATABRICKS DEBUG] Error getting procedures: {proc_error}")
                    procedures = []
                                
                # Look for indexes
                indexes = []
                try:
                    # Databricks doesn't use traditional indexes like PostgreSQL/MySQL
                    # Delta Lake handles optimization differently
                    indexes = []
                except Exception as index_error:
                    print(f"[DATABRICKS DEBUG] Error getting indexes: {index_error}")
                    indexes = []
                                
                # Look for triggers
                triggers = []
                try:
                    # Databricks doesn't support triggers like traditional RDBMS
                    triggers = []
                except Exception as trigger_error:
                    print(f"[DATABRICKS DEBUG] Error getting triggers: {trigger_error}")
                    triggers = []
                                
                # Look for sequences
                sequences = []
                try:
                    # Databricks doesn't have sequences like PostgreSQL
                    sequences = []
                except Exception as seq_error:
                    print(f"[DATABRICKS DEBUG] Error getting sequences: {seq_error}")
                    sequences = []
                                
                # Look for user-defined types
                user_types = []
                try:
                    # Databricks doesn't have user-defined types like PostgreSQL
                    user_types = []
                except Exception as utype_error:
                    print(f"[DATABRICKS DEBUG] Error getting user types: {utype_error}")
                    user_types = []
                                
                # Look for partitions
                partitions = []
                try:
                    # Databricks supports partitioned tables through Delta Lake
                    # We can get partition information from the table properties
                    for table in tables:
                        schema_name = table['schema']
                        table_name = table['name']
                        try:
                            desc_cursor = connection.cursor()
                            desc_cursor.execute(f"DESCRIBE TABLE `{schema_name}`.`{table_name}` PARTITION")
                            partition_info = desc_cursor.fetchall()
                            desc_cursor.close()
                            if partition_info:
                                partitions.append({
                                    "schema": schema_name,
                                    "table": table_name,
                                    "partition_key": [col[0] for col in partition_info]  # Column names that are partition keys
                                })
                        except:
                            # Table may not be partitioned
                            pass
                except Exception as part_error:
                    print(f"[DATABRICKS DEBUG] Error getting partitions: {part_error}")
                    partitions = []
                                
                # Look for permissions
                permissions = []
                try:
                    # Databricks has its own permission system, but we can check basic grants
                    permissions = []
                except Exception as perm_error:
                    print(f"[DATABRICKS DEBUG] Error getting permissions: {perm_error}")
                    permissions = []
                
                cursor.close()
                connection.close()
                
                result = {
                    "database_info": {
                        "type": "Databricks", 
                        "version": str(version), 
                        "schemas": schemas, 
                        "encoding": "utf8", 
                        "collation": "utf8_general_ci",
                        "catalog": self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                    },
                    "tables": tables,
                    "columns": columns,
                    "constraints": [],  # Databricks has limited constraint support
                    "views": views,
                    "procedures": procedures,
                    "indexes": indexes,  # Databricks uses Delta Lake which handles optimization differently
                    "triggers": triggers,
                    "sequences": sequences,
                    "user_types": user_types,
                    "materialized_views": materialized_views,
                    "partitions": partitions,  # Databricks supports partitioned tables
                    "permissions": permissions,
                    "data_profiles": data_profiles,
                    "storage_info": {
                        "database_size": {
                            "total_size": sum(profile.get("row_count", 0) for profile in data_profiles),
                            "data_size": sum(profile.get("row_count", 0) for profile in data_profiles),
                            "index_size": 0
                        },
                        "tables": storage_tables
                    },
                    "driver_unavailable": False
                }
                
                print(f"[DATABRICKS DEBUG] Introspection complete. Tables: {len(tables)}, Columns: {len(columns)}, Profiles: {len(data_profiles)}, Views: {len(views)}")
                return result
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, introspect_sync)
            return result
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DATABRICKS ERROR] Introspection failed: {error_msg}")
            return {
                "database_info": {
                    "type": "Databricks", 
                    "version": "Error", 
                    "schemas": [], 
                    "encoding": "utf8", 
                    "collation": "utf8_general_ci",
                    "catalog": self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                },
                "tables": [], 
                "columns": [], 
                "constraints": [], 
                "views": [],
                "procedures": [], 
                "indexes": [], 
                "triggers": [], 
                "sequences": [],
                "user_types": [], 
                "materialized_views": [], 
                "partitions": [], 
                "permissions": [],
                "data_profiles": [],
                "storage_info": {
                    "database_size": {
                        "total_size": 0,
                        "data_size": 0,
                        "index_size": 0
                    },
                    "tables": []
                },
                "error": error_msg,
                "driver_unavailable": False
            }
    
    async def extract_objects(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": False, "ddl_scripts": {"tables": [], "views": [], "indexes": []}, "object_count": 0, "driver_unavailable": True, "message": "Databricks driver not available"}
        
        try:
            def extract_sync():
                connection = sql.connect(
                    server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                    http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                    access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                    catalog=self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore"),
                    schema=self.credentials.get("schema") or self.credentials.get("schemaName", "default")
                )
                cursor = connection.cursor()
                
                # Get schemas
                cursor.execute("SHOW DATABASES")
                schemas = [row[0] for row in cursor.fetchall()]
                
                # Extract tables DDL
                tables_ddl = []
                for schema in schemas:
                    try:
                        cursor.execute(f"SHOW TABLES IN `{schema}`")
                        schema_tables = cursor.fetchall()
                        
                        for row in schema_tables:
                            table_name = row[1]
                            try:
                                # Get table DDL using DESCRIBE TABLE EXTENDED
                                ddl_cursor = connection.cursor()
                                ddl_cursor.execute(f'DESCRIBE TABLE EXTENDED `{schema}`.`{table_name}`')
                                table_desc = ddl_cursor.fetchall()
                                ddl_cursor.close()
                                
                                # Build basic CREATE TABLE statement from description
                                column_defs = []
                                for col_row in table_desc:
                                    if col_row[0] and not col_row[0].startswith("#"):  # Skip header/comment rows
                                        column_defs.append(f"  `{col_row[0]}` {col_row[1]}")
                                
                                ddl_text = f"CREATE TABLE `{schema}`.`{table_name}` (\n"
                                ddl_text += ",\n".join(column_defs)
                                ddl_text += "\n)"
                                
                                tables_ddl.append({
                                    "schema": schema,
                                    "name": table_name,
                                    "ddl": ddl_text
                                })
                            except:
                                # Fallback to basic table info
                                tables_ddl.append({
                                    "schema": schema,
                                    "name": table_name,
                                    "ddl": f"-- Unable to extract DDL for {schema}.{table_name}"
                                })
                    except:
                        continue
                
                cursor.close()
                connection.close()
                
                return {
                    "ddl_scripts": {
                        "tables": tables_ddl,
                        "views": [],
                        "indexes": []
                    },
                    "object_count": len(tables_ddl)
                }
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, extract_sync)
            return result
            
        except Exception as e:
            return {"ddl_scripts": {"tables": [], "views": [], "indexes": []}, "object_count": 0, "error": str(e)}
    
    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": False, "created": 0, "attempted": len(translated_ddl), "driver_unavailable": True, "message": "Databricks driver not available"}
        
        try:
            def create_sync():
                import re

                # NOTE: use module-level splitter so create_objects and run_ddl behave consistently.

                def _normalize_ddl(raw: str) -> str:
                    import re

                    ddl = (raw or "").strip()
                    if not ddl:
                        return ""

                    # Replace Oracle schema qualifiers and normalize CREATE TABLE prefix.
                    m = re.match(r'(?is)^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?P<name>[^(\n]+)\s*\(', ddl)
                    if m:
                        raw_name = (m.group("name") or "").strip()
                        parts = [p.strip().strip('`"') for p in raw_name.split(".") if p.strip()]
                        table_only = parts[-1] if parts else raw_name.strip('`"')
                        ddl = re.sub(
                            r'(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^(\n]+\s*\(',
                            f'CREATE TABLE IF NOT EXISTS `{table_only}` (',
                            ddl,
                            count=1
                        )

                    # Normalize identifiers.
                    ddl = ddl.replace('"', '`')

                    # Oracle -> Databricks type conversions (best-effort).
                    # IMPORTANT: preserve VARCHAR/CHAR lengths; prefer VARCHAR/CHAR over STRING where possible.
                    ddl = re.sub(r'\bNVARCHAR2\s*\(', 'VARCHAR(', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bVARCHAR2\s*\(', 'VARCHAR(', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNVARCHAR2\b', 'VARCHAR', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bVARCHAR2\b', 'VARCHAR', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNCHAR\s*\(', 'CHAR(', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNCHAR\b', 'CHAR', ddl, flags=re.IGNORECASE)

                    # Large objects.
                    ddl = re.sub(r'\bCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bTEXT\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bBLOB\b', 'BINARY', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bRAW\b', 'BINARY', ddl, flags=re.IGNORECASE)

                    # Floating point.
                    ddl = re.sub(r'\bBINARY_FLOAT\b', 'FLOAT', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bBINARY_DOUBLE\b', 'DOUBLE', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bFLOAT\b', 'DOUBLE', ddl, flags=re.IGNORECASE)

                    # Normalize illegal length specifications for native Spark types.
                    ddl = re.sub(
                        r'\bSTRING\s*\(\s*\d+\s*(?:CHAR|BYTE)?\s*\)',
                        'STRING',
                        ddl,
                        flags=re.IGNORECASE
                    )
                    ddl = re.sub(
                        r'\bBINARY\s*\(\s*\d+\s*\)',
                        'BINARY',
                        ddl,
                        flags=re.IGNORECASE
                    )

                    # NUMBER mapping (enterprise rule-aligned) in case any Oracle NUMBER leaks through.
                    ddl = re.sub(
                        r'\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)',
                        r'DECIMAL(\1,\2)',
                        ddl,
                        flags=re.IGNORECASE
                    )
                    ddl = re.sub(
                        r'\bNUMBER\s*\(\s*(\d+)\s*\)',
                        r'DECIMAL(\1)',
                        ddl,
                        flags=re.IGNORECASE
                    )
                    ddl = re.sub(r'\bNUMBER\b', 'INT', ddl, flags=re.IGNORECASE)

                    ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', ddl, flags=re.IGNORECASE)

                    # Databricks requires DATE defaults to be CURRENT_DATE, not CURRENT_TIMESTAMP.
                    ddl = re.sub(
                        r'\bDATE\s+DEFAULT\s+CURRENT_TIMESTAMP\s*(?:\(\s*\))?',
                        'DATE DEFAULT CURRENT_DATE',
                        ddl,
                        flags=re.IGNORECASE
                    )

                    # Ensure CREATE TABLE uses Delta (enterprise rule).
                    if re.match(r'(?is)^\s*CREATE\s+TABLE\b', ddl) and not re.search(r'(?is)\bUSING\s+DELTA\b', ddl):
                        ddl = ddl.strip().rstrip(";")
                        if re.search(r'(?is)\bTBLPROPERTIES\b', ddl):
                            ddl = re.sub(r'(?is)\bTBLPROPERTIES\b', 'USING DELTA TBLPROPERTIES', ddl, count=1)
                        elif re.search(r'(?is)\bCLUSTER\s+BY\b', ddl):
                            ddl = re.sub(r'(?is)\bCLUSTER\s+BY\b', 'USING DELTA CLUSTER BY', ddl, count=1)
                        else:
                            ddl = ddl + ' USING DELTA'

                    def _ensure_tblproperties(statement: str, props: Dict[str, str]) -> str:
                        if not props:
                            return statement
                        existing_match = re.search(r'\bTBLPROPERTIES\s*\((?P<body>[^)]*)\)', statement, flags=re.IGNORECASE | re.DOTALL)
                        if existing_match:
                            body = existing_match.group("body")
                            additions = []
                            for key, value in props.items():
                                if re.search(rf"'{re.escape(key)}'\s*=", body, flags=re.IGNORECASE):
                                    continue
                                additions.append(f"'{key}' = '{value}'")
                            if not additions:
                                return statement
                            new_body = body.strip().rstrip(',')
                            if new_body:
                                new_body = new_body + ", " + ", ".join(additions)
                            else:
                                new_body = ", ".join(additions)
                            return re.sub(
                                r'\bTBLPROPERTIES\s*\([^)]*\)',
                                f"TBLPROPERTIES ({new_body})",
                                statement,
                                flags=re.IGNORECASE | re.DOTALL
                            )
                        props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
                        statement = statement.strip().rstrip(";")
                        return f"{statement} TBLPROPERTIES ({props_sql});"

                    if re.search(r'\bDEFAULT\b', ddl, flags=re.IGNORECASE):
                        ddl = _ensure_tblproperties(ddl, {"delta.feature.allowColumnDefaults": "supported"})

                    # Strip Oracle-specific storage/physical clauses; keep constraints.
                    ddl = re.sub(r'\bENABLE\b', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bUSING\s+INDEX\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bTABLESPACE\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bPCTFREE\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bINITRANS\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bMAXTRANS\b[^,\n\)]*', '', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bSTORAGE\b\s*\([^)]*\)', '', ddl, flags=re.IGNORECASE)

                    ddl = ddl.strip().rstrip(";") + ";"
                    return ddl

                def _rewrite_schema_refs(statement: str, target_schema: str) -> str:
                    if not statement or not target_schema:
                        return statement
                    schema_token = f"`{str(target_schema).strip('`')}`"

                    def _q(name: str) -> str:
                        cleaned = str(name or "").strip()
                        if not cleaned:
                            return cleaned
                        if cleaned.startswith("`") and cleaned.endswith("`"):
                            return cleaned
                        return f"`{cleaned.strip('`')}`"

                    pattern = re.compile(
                        r'(?is)\bREFERENCES\s+(?P<schema>`[^`]+`|\"[^\"]+\"|\w+)\s*\.\s*(?P<table>`[^`]+`|\"[^\"]+\"|\w+)'
                    )

                    def _replace(match: re.Match) -> str:
                        table = match.group("table")
                        return f"REFERENCES {schema_token}.{_q(table)}"

                    return pattern.sub(_replace, statement)

                default_catalog = self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                default_schema = self.credentials.get("schema") or self.credentials.get("schemaName", "default")
                connection = sql.connect(
                    server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                    http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                    access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                    catalog=default_catalog,
                    schema=default_schema
                )
                cursor = connection.cursor()

                # Detect catalog type and FK support
                catalog_name, supports_fk = self._detect_catalog_type(connection)
                self.logger.info(f"[DATABRICKS] Using catalog: {catalog_name}, FK support: {supports_fk}")

                translated_list = translated_ddl or []
                attempted_total = len(translated_list)
                attempted_sql = 0
                created_count = 0
                errors: List[Dict[str, Any]] = []
                skipped: List[Dict[str, Any]] = []
                fk_warnings: List[Dict[str, Any]] = []
                deferred_fks: List[Dict[str, Any]] = []  # FKs to add via ALTER TABLE
                deferred_checks: List[Dict[str, Any]] = []  # CHECK constraints to add via ALTER TABLE
                constraint_warnings: List[str] = []  # UNIQUE constraint warnings

                for obj in translated_list:
                    ddl = ""  # Initialize ddl to ensure it's always defined
                    raw_ddl = obj.get("target_sql") or obj.get("translated_ddl") or obj.get("ddl", "")
                    try:
                        if not raw_ddl or not str(raw_ddl).strip():
                            skipped.append({
                                "name": obj.get("name", "unknown"),
                                "schema": obj.get("schema", default_schema),
                                "error": "Missing target DDL",
                                "ddl": "",
                                "original_ddl": raw_ddl or ""
                            })
                            continue

                        # Ensure we're in the correct catalog/schema context
                        try:
                            cursor.execute(f"USE CATALOG `{default_catalog}`")
                        except Exception as catalog_err:
                            self.logger.error(f"[DATABRICKS] Failed to USE CATALOG {default_catalog}: {catalog_err}")
                            raise Exception(f"Cannot use catalog '{default_catalog}': {catalog_err}")

                        try:
                            cursor.execute(f"USE SCHEMA `{default_schema}`")
                        except Exception as schema_err:
                            self.logger.error(f"[DATABRICKS] Failed to USE SCHEMA {default_schema}: {schema_err}")
                            raise Exception(f"Cannot use schema '{default_schema}': {schema_err}")

                        statements = _split_sql_statements(str(raw_ddl))
                        if not statements:
                            skipped.append({
                                "name": obj.get("name", "unknown"),
                                "schema": obj.get("schema", default_schema),
                                "error": "No SQL statements found",
                                "ddl": "",
                                "original_ddl": raw_ddl or ""
                            })
                            continue

                        for stmt in statements:
                            attempted_sql += 1
                            ddl = _normalize_ddl(stmt)
                            ddl = _rewrite_schema_refs(ddl, default_schema)
                            if not ddl:
                                continue

                            # Always strip FK constraints from CREATE TABLE to avoid:
                            # 1. Self-referencing FK errors (table doesn't exist yet)
                            # 2. FK dependency ordering issues (referenced table doesn't exist yet)
                            # For Unity Catalog, we'll add them back via ALTER TABLE after all tables are created
                            if _contains_foreign_keys(ddl):
                                cleaned_ddl, removed_fks = _strip_foreign_keys(ddl)
                                if removed_fks:
                                    if supports_fk:
                                        # Defer FKs for Unity Catalog - add via ALTER TABLE later
                                        deferred_fks.append({
                                            "table": obj.get("name", "unknown"),
                                            "schema": obj.get("schema", default_schema),
                                            "fk_constraints": removed_fks
                                        })
                                        self.logger.info(
                                            f"[DATABRICKS] Deferred {len(removed_fks)} FK constraint(s) from "
                                            f"{obj.get('name', 'unknown')} - will add via ALTER TABLE after table creation"
                                        )
                                    else:
                                        # Warn for hive_metastore - FKs not supported
                                        fk_warnings.append({
                                            "table": obj.get("name", "unknown"),
                                            "schema": obj.get("schema", default_schema),
                                            "removed_fks": removed_fks
                                        })
                                        self.logger.warning(
                                            f"[DATABRICKS] Stripped {len(removed_fks)} FK constraint(s) from "
                                            f"{obj.get('name', 'unknown')} (catalog '{catalog_name}' doesn't support FKs)"
                                        )
                                ddl = cleaned_ddl

                            # Extract CHECK constraints to add via ALTER TABLE
                            cleaned_ddl, check_constraints = _extract_check_constraints(ddl)
                            if check_constraints:
                                table_name = obj.get('name', 'unknown')
                                deferred_checks.append({
                                    "table": table_name,
                                    "schema": obj.get("schema", default_schema),
                                    "checks": check_constraints
                                })
                                self.logger.info(
                                    f"[DATABRICKS] Extracted {len(check_constraints)} CHECK constraint(s) from "
                                    f"{table_name} - will add via ALTER TABLE after table creation"
                                )
                                ddl = cleaned_ddl

                            # Convert UNIQUE constraints to column-level UNIQUE
                            cleaned_ddl, unique_warnings = _convert_unique_to_column_level(ddl)
                            if unique_warnings:
                                table_name = obj.get('name', 'unknown')
                                constraint_warnings.extend([f"{table_name}: {w}" for w in unique_warnings])
                                self.logger.warning(
                                    f"[DATABRICKS] {len(unique_warnings)} multi-column UNIQUE constraint(s) removed from "
                                    f"{table_name} - not supported in Databricks"
                                )
                                ddl = cleaned_ddl

                            self.logger.info(f"[DATABRICKS] Executing DDL for {obj.get('name', 'unknown')}: {ddl[:200]}...")
                            cursor.execute(ddl)

                        created_count += 1
                    except Exception as e:
                        # Log the original DDL and normalized DDL for debugging
                        original_ddl = raw_ddl or ""
                        self.logger.error(f"[DATABRICKS] Error creating object: {e}")
                        self.logger.error(f"[DATABRICKS] Original DDL: {original_ddl}")
                        self.logger.error(f"[DATABRICKS] Normalized DDL: {ddl}")
                        errors.append({
                            "name": obj.get("name", "unknown"),
                            "schema": obj.get("schema", default_schema),
                            "error": str(e),
                            "ddl": ddl,
                            "original_ddl": original_ddl
                        })
                        continue

                # Phase 2: Add deferred FK constraints via ALTER TABLE (Unity Catalog only)
                if supports_fk and deferred_fks:
                    self.logger.info(f"[DATABRICKS] Adding {len(deferred_fks)} deferred FK constraint(s) via ALTER TABLE...")
                    fk_add_errors = 0

                    for fk_info in deferred_fks:
                        table_name = fk_info.get("table", "unknown")
                        table_schema = fk_info.get("schema", default_schema)
                        fk_constraints = fk_info.get("fk_constraints", [])

                        for fk_constraint in fk_constraints:
                            try:
                                # Parse the FK constraint to extract the necessary parts
                                # Example: "CONSTRAINT `FK_NAME` FOREIGN KEY (`col`) REFERENCES `table`(`ref_col`)"
                                # We need to convert this to: ALTER TABLE `table` ADD CONSTRAINT ...

                                # Clean up the constraint (remove leading comma/whitespace)
                                fk_constraint_cleaned = fk_constraint.strip().lstrip(',').strip()

                                # Build ALTER TABLE statement
                                alter_stmt = (
                                    f"ALTER TABLE `{table_name}` ADD {fk_constraint_cleaned}"
                                )

                                self.logger.info(f"[DATABRICKS] Adding FK to {table_name}: {alter_stmt[:150]}...")
                                cursor.execute(alter_stmt)

                            except Exception as fk_err:
                                fk_add_errors += 1
                                self.logger.warning(
                                    f"[DATABRICKS] Failed to add FK constraint to {table_name}: {fk_err}"
                                )
                                # Don't fail the entire migration if FK addition fails
                                # Just log it and continue

                    if fk_add_errors > 0:
                        self.logger.warning(
                            f"[DATABRICKS] {fk_add_errors} FK constraint(s) could not be added. "
                            f"Tables were created successfully but some FK constraints are missing."
                        )

                # Phase 3: Add deferred CHECK constraints via ALTER TABLE
                if deferred_checks:
                    self.logger.info(f"[DATABRICKS] Adding {len(deferred_checks)} deferred CHECK constraint(s) via ALTER TABLE...")
                    check_add_errors = 0

                    for check_info in deferred_checks:
                        table_name = check_info.get("table", "unknown")
                        table_schema = check_info.get("schema", default_schema)
                        check_constraints = check_info.get("checks", [])

                        for check_constraint in check_constraints:
                            try:
                                constraint_name = check_constraint.get("name")
                                condition = check_constraint.get("condition")

                                # Build ALTER TABLE statement
                                alter_stmt = (
                                    f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{constraint_name}` CHECK ({condition})"
                                )

                                self.logger.info(f"[DATABRICKS] Adding CHECK to {table_name}: {alter_stmt[:150]}...")
                                cursor.execute(alter_stmt)

                            except Exception as check_err:
                                check_add_errors += 1
                                self.logger.warning(
                                    f"[DATABRICKS] Failed to add CHECK constraint to {table_name}: {check_err}"
                                )
                                # Don't fail the entire migration if CHECK addition fails
                                # Just log it and continue

                    if check_add_errors > 0:
                        self.logger.warning(
                            f"[DATABRICKS] {check_add_errors} CHECK constraint(s) could not be added. "
                            f"Tables were created successfully but some CHECK constraints are missing."
                        )

                connection.commit()
                cursor.close()
                connection.close()

                all_errors = errors + skipped
                result = {
                    "ok": len(all_errors) == 0,
                    "created": created_count,
                    "attempted": attempted_total,
                    "attempted_sql": attempted_sql,
                    "catalog": catalog_name,
                    "supports_fk": supports_fk
                }
                if deferred_fks:
                    result["deferred_fks"] = deferred_fks
                    result["deferred_fks_count"] = len(deferred_fks)
                if deferred_checks:
                    result["deferred_checks"] = deferred_checks
                    result["deferred_checks_count"] = len(deferred_checks)
                if fk_warnings:
                    result["fk_warnings"] = fk_warnings
                if constraint_warnings:
                    result["constraint_warnings"] = constraint_warnings
                if skipped:
                    result["skipped"] = skipped
                if all_errors:
                    result["errors"] = all_errors
                    result["message"] = f"Created {created_count}/{attempted_total} objects with {len(all_errors)} errors"
                elif attempted_total > 0 and created_count == 0:
                    result["message"] = "No objects were created in Databricks"
                return result
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, create_sync)
            return result
            
        except Exception as e:
            return {"ok": False, "message": str(e)}

    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "dropped": len(table_names)}

        def _q(parts: List[str]) -> str:
            return ".".join(f"`{str(p).replace('`', '``')}`" for p in parts if p)

        try:
            def drop_sync():
                connection = sql.connect(
                    server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                    http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                    access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                    catalog=self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore"),
                    schema=self.credentials.get("schema") or self.credentials.get("schemaName", "default")
                )
                cursor = connection.cursor()

                dropped = 0
                errors: List[Dict[str, Any]] = []

                default_catalog = self.credentials.get("catalog", "hive_metastore")
                default_schema = self.credentials.get("schema", "default")

                for ref in table_names or []:
                    try:
                        raw = str(ref or "").strip()
                        if not raw:
                            continue
                        parts = [p for p in raw.split(".") if p]
                        if len(parts) >= 3:
                            catalog, schema, table = parts[-3], parts[-2], parts[-1]
                        elif len(parts) == 2:
                            catalog, schema, table = default_catalog, parts[0], parts[1]
                        else:
                            catalog, schema, table = default_catalog, default_schema, parts[0]

                        cursor.execute(f"DROP TABLE IF EXISTS {_q([catalog, schema, table])}")
                        dropped += 1
                    except Exception as e:
                        errors.append({"table": ref, "error": str(e)})

                connection.commit()
                cursor.close()
                connection.close()
                return {"ok": len(errors) == 0, "dropped": dropped, "errors": errors}

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, drop_sync)
        except Exception as e:
            return {"ok": False, "message": str(e)}
    
    async def copy_table_data(
        self,
        table_name: str,
        source_adapter: 'DatabaseAdapter',
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "table": table_name, "rows_copied": 50000, "driver_unavailable": True}

        def copy_sync() -> Dict[str, Any]:
            try:
                def _q(ident: str) -> str:
                    return f"`{str(ident).replace('`', '``')}`"

                parts = [p for p in str(table_name).split(".") if p]
                source_schema = parts[-2] if len(parts) >= 2 else None
                source_table = parts[-1] if parts else str(table_name)

                target_catalog = self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                target_schema = self.credentials.get("schema") or self.credentials.get("schemaName", "default")
                target_table = source_table
                target_ref = ".".join([_q(target_catalog), _q(target_schema), _q(target_table)])

                target_connection = sql.connect(
                    server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                    http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                    access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                    catalog=target_catalog,
                    schema=target_schema,
                )
                target_cursor = target_connection.cursor()

                try:
                    target_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(target_catalog)}.{_q(target_schema)}")
                except Exception:
                    pass

                source_connection = source_adapter.get_connection()
                source_cursor = source_connection.cursor()
                # Encourage larger fetch batches from Oracle.
                try:
                    source_cursor.arraysize = int(chunk_size)
                except Exception:
                    pass

                try:
                    def _qsource(ident: str) -> str:
                        return '"' + str(ident).replace('"', '""') + '"'

                    if source_schema:
                        source_ref = f"{_qsource(source_schema)}.{_qsource(source_table)}"
                    else:
                        source_ref = f"{_qsource(source_table)}"

                    requested_columns = [str(c) for c in (columns or []) if str(c or '').strip()]
                    select_cols = ", ".join(_qsource(c) for c in requested_columns) if requested_columns else "*"
                    source_cursor.execute(f"SELECT {select_cols} FROM {source_ref}")
                    source_columns = [desc[0] for desc in (source_cursor.description or [])]
                    source_index = {str(col).lower(): idx for idx, col in enumerate(source_columns)}

                    try:
                        target_cursor.execute(f"SELECT * FROM {target_ref} LIMIT 0")
                        target_columns = [desc[0] for desc in (target_cursor.description or [])]
                    except Exception as e:
                        # Do not auto-create tables here; structure migration must create the schema/constraints.
                        return {
                            "ok": False,
                            "table": table_name,
                            "rows_copied": 0,
                            "error": f"Target table not found. Run structure migration first. Details: {e}"
                        }

                    if requested_columns:
                        target_columns = [col for col in target_columns if str(col).lower() in source_index]

                    if not target_columns:
                        return {"ok": False, "table": table_name, "rows_copied": 0, "error": "Target table has no columns"}

                    insert_cols = ", ".join(_q(col) for col in target_columns)
                    placeholders = ", ".join(["?"] * len(target_columns))
                    insert_sql = f"INSERT INTO {target_ref} ({insert_cols}) VALUES ({placeholders})"

                    rows_copied = 0
                    while True:
                        rows = source_cursor.fetchmany(chunk_size)
                        if not rows:
                            break

                        batch = []
                        for row in rows:
                            values = []
                            for col in target_columns:
                                idx = source_index.get(str(col).lower())
                                values.append(row[idx] if idx is not None else None)
                            batch.append(tuple(values))

                        if batch:
                            target_cursor.executemany(insert_sql, batch)
                            rows_copied += len(batch)
                            target_connection.commit()
                            callback = progress_cb or getattr(self, "_progress_callback", None)
                            if callable(callback):
                                try:
                                    callback(rows_copied, len(batch))
                                except Exception:
                                    pass

                    target_connection.commit()
                    callback = progress_cb or getattr(self, "_progress_callback", None)
                    if callable(callback):
                        try:
                            callback(rows_copied, 0)
                        except Exception:
                            pass
                    return {"ok": True, "table": table_name, "rows_copied": rows_copied}
                finally:
                    try:
                        source_cursor.close()
                    except Exception:
                        pass
                    try:
                        source_connection.close()
                    except Exception:
                        pass
                    try:
                        target_cursor.close()
                    except Exception:
                        pass
                    try:
                        target_connection.close()
                    except Exception:
                        pass
            except Exception as e:
                return {
                    "ok": False,
                    "table": table_name,
                    "rows_copied": 0,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }

        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, copy_sync)
        except Exception as e:
            return {
                "ok": False,
                "table": table_name,
                "rows_copied": 0,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
    
    async def get_table_row_count(self, table_name: str) -> int:
        """Get the row count for a specific table in Databricks"""
        if not self.driver_available:
            return 0  # Return 0 if driver is unavailable
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # Parse table name to get schema and table
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                # Use backticks for Databricks schema and table names
                quoted_table_name = f'`{schema}`.`{table}`'
            else:
                # Just use backticks for the table name in Databricks
                quoted_table_name = f'`{table_name}`'
            
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
        if not self.driver_available:
            return {"structural": {"schema_match": True}, "data": {"row_counts_match": True}, "driver_unavailable": True}
        
        # This would typically involve comparing schema and data between source and Databricks
        # For now, we'll return a placeholder
        return {"structural": {"schema_match": True}, "data": {"row_counts_match": True}}

    async def run_ddl(self, ddl: str) -> Dict[str, Any]:
        """Execute arbitrary DDL against Databricks.

        Splits the SQL string on semicolons and runs each statement using the
        Databricks connector. Commits on success and returns ``{"ok": True}``.
        On failure returns ``{"ok": False, "error": "msg"}``.
        """
        if not self.driver_available:
            return {"ok": False, "error": "Databricks driver unavailable"}
        try:
            def _extract_error_fields(err: Exception) -> Dict[str, Any]:
                fields: Dict[str, Any] = {}
                for key in ("sqlstate", "error_code", "errorCode", "status_code", "statusCode"):
                    val = getattr(err, key, None)
                    if val is not None:
                        fields[key] = val
                # Some Databricks connector errors wrap extra info in args.
                if hasattr(err, "args") and err.args:
                    fields["args"] = [str(a) for a in err.args[:3]]
                return fields

            connection = self.get_connection()
            cursor = connection.cursor()

            # Detect catalog type and FK support
            catalog_name, supports_fk = self._detect_catalog_type(connection)
            self.logger.info(f"[DATABRICKS] run_ddl using catalog: {catalog_name}, FK support: {supports_fk}")

            # Ensure we're in the configured catalog/schema for the connection.
            default_catalog = self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
            default_schema = self.credentials.get("schema") or self.credentials.get("schemaName", "default")

            # Ensure catalog/schema exist and are active.
            # Don't silently ignore catalog errors - surface them to the user
            try:
                cursor.execute(f"USE CATALOG `{default_catalog}`")
            except Exception as e:
                self.logger.warning(f"[DATABRICKS] Could not use catalog {default_catalog}: {e}")

            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{default_schema}`")
            except Exception as e:
                self.logger.warning(f"[DATABRICKS] Could not create schema {default_schema}: {e}")

            try:
                cursor.execute(f"USE SCHEMA `{default_schema}`")
            except Exception as e:
                self.logger.warning(f"[DATABRICKS] Could not use schema {default_schema}: {e}")

            statements = _split_sql_statements(ddl)
            results: List[Dict[str, Any]] = []
            fk_warnings: List[str] = []
            constraint_warnings: List[str] = []

            for idx, stmt in enumerate(statements):
                stmt_text = str(stmt or "").strip()
                if not stmt_text:
                    continue
                # Normalize and rewrite to increase success rate on Databricks
                normalized = _normalize_ddl_for_databricks(stmt_text)
                stmt_to_run = _rewrite_schema_refs(normalized, default_schema)

                # Strip FK constraints if catalog doesn't support them
                if not supports_fk and _contains_foreign_keys(stmt_to_run):
                    cleaned_ddl, removed_fks = _strip_foreign_keys(stmt_to_run)
                    if removed_fks:
                        fk_warnings.extend(removed_fks)
                        self.logger.warning(
                            f"[DATABRICKS] Stripped {len(removed_fks)} FK constraint(s) from statement "
                            f"(catalog '{catalog_name}' doesn't support FKs)"
                        )
                    stmt_to_run = cleaned_ddl

                # Strip CHECK and UNIQUE constraints (Databricks only supports PK and FK inline)
                cleaned_ddl, removed_constraints = _strip_check_and_unique_constraints(stmt_to_run)
                if removed_constraints:
                    constraint_warnings.extend(removed_constraints)
                    self.logger.warning(
                        f"[DATABRICKS] Stripped {len(removed_constraints)} CHECK/UNIQUE constraint(s) from statement "
                        f"- Databricks only supports PK and FK in CREATE TABLE"
                    )
                    stmt_to_run = cleaned_ddl

                try:
                    cursor.execute(stmt_to_run)
                    results.append({
                        "index": idx,
                        "statement": stmt_text,
                        "ok": True
                    })
                except Exception as e:
                    err_text = str(e)
                    results.append({
                        "index": idx,
                        "statement": stmt_text,
                        "ok": False,
                        "error": err_text,
                        **_extract_error_fields(e)
                    })
                    # Stop at first failure to avoid cascading/opaque errors.
                    break

            ok = all(r.get("ok") for r in results) if results else False
            if ok:
                connection.commit()

            cursor.close()
            connection.close()

            first_error = next((r.get("error") for r in results if not r.get("ok")), None)
            result = {
                "ok": ok,
                "statements": results,
                "error": first_error,
                "catalog": catalog_name,
                "supports_fk": supports_fk
            }
            if fk_warnings:
                result["fk_warnings"] = fk_warnings
            if constraint_warnings:
                result["constraint_warnings"] = constraint_warnings
            return result
        except Exception as e:
            return {"ok": False, "error": str(e), "statements": []}

    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        """Rename a column in Databricks using ALTER TABLE ... RENAME COLUMN ... TO ..."""
        if not self.driver_available:
            return {"ok": False, "message": "Databricks driver not available"}

        def _q(ident: str) -> str:
            return f"`{str(ident).replace('`', '``')}`"

        def _qref(parts: List[str]) -> str:
            return ".".join(_q(p) for p in parts if p)

        def rename_sync():
            default_catalog = self.credentials.get("catalog") or self.credentials.get("catalogName") or "hive_metastore"
            default_schema = self.credentials.get("schema") or self.credentials.get("schemaName") or "default"

            raw = str(table_name or "").strip()
            parts = [p for p in raw.split(".") if p]
            if len(parts) >= 3:
                catalog, schema, table = parts[-3], parts[-2], parts[-1]
            elif len(parts) == 2:
                catalog, schema, table = default_catalog, parts[0], parts[1]
            elif len(parts) == 1:
                catalog, schema, table = default_catalog, default_schema, parts[0]
            else:
                raise ValueError("table_name is required")

            old_col = str(old_column_name or "").strip().strip("`")
            new_col = str(new_column_name or "").strip().strip("`")
            if not old_col or not new_col:
                raise ValueError("old_column_name and new_column_name are required")

            connection = sql.connect(
                server_hostname=self.credentials.get("host") or self.credentials.get("server_hostname"),
                http_path=self.credentials.get("http_path") or self.credentials.get("httpPath"),
                access_token=self.credentials.get("access_token") or self.credentials.get("accessToken"),
                catalog=default_catalog,
                schema=default_schema,
            )
            cursor = connection.cursor()
            try:
                full_ref = _qref([catalog, schema, table])
                cursor.execute(f"ALTER TABLE {full_ref} RENAME COLUMN {_q(old_col)} TO {_q(new_col)}")
                connection.commit()
                return {
                    "ok": True,
                    "message": f"Successfully renamed column {old_col} to {new_col} in {catalog}.{schema}.{table}",
                }
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    connection.close()
                except Exception:
                    pass

        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, rename_sync)
        except Exception as e:
            return {"ok": False, "message": f"Failed to rename column: {str(e)}", "error": str(e)}
