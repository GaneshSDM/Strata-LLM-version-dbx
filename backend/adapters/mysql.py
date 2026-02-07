from typing import Dict, Any, List, Optional, Callable
import traceback
from .base import DatabaseAdapter

try:
    import mysql.connector
    DRIVER_AVAILABLE = True
except ImportError:
    DRIVER_AVAILABLE = False

class MySQLAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        self.driver_available = DRIVER_AVAILABLE
    
    def _quote_identifier(self, identifier: str) -> str:
        """Safely quote MySQL identifiers by escaping backticks and wrapping in backticks."""
        escaped = identifier.replace('`', '``')
        return f'`{escaped}`'

    def _strip_db_qualifier_from_create_table(self, ddl: str, db_name: str, table_name: str) -> str:
        """
        MySQL `SHOW CREATE TABLE` often returns `CREATE TABLE `db`.`table` ...`.
        For MySQL -> Snowflake migration the target schema is fixed, so we strip the db qualifier
        to avoid accidentally creating per-source schemas in Snowflake.
        """
        import re

        ddl_text = str(ddl or "")
        if not ddl_text:
            return ddl_text
        db = str(db_name or "").strip().strip("`").strip('"')
        table = str(table_name or "").strip().strip("`").strip('"')
        if not db or not table:
            return ddl_text

        # Handle both backtick and double-quote variants defensively.
        patterns = [
            (rf'(?is)^\s*CREATE\s+TABLE\s+`{re.escape(db)}`\s*\.\s*`{re.escape(table)}`\s*\(', rf'CREATE TABLE `{table}` ('),
            (rf'(?is)^\s*CREATE\s+TABLE\s+"{re.escape(db)}"\s*\.\s*"{re.escape(table)}"\s*\(', rf'CREATE TABLE "{table}" ('),
            (rf'(?is)^\s*CREATE\s+TABLE\s+{re.escape(db)}\s*\.\s*{re.escape(table)}\s*\(', rf'CREATE TABLE {table} ('),
        ]
        for pat, repl in patterns:
            new_text, n = re.subn(pat, repl, ddl_text, count=1)
            if n:
                return new_text
        return ddl_text

    def get_connection(self):
        """Return a synchronous mysql.connector connection for validation helpers."""
        if not self.driver_available:
            raise RuntimeError("MySQL driver (mysql-connector-python) is not installed")

        import mysql.connector  # type: ignore
        conn_params = {
            "host": self.credentials.get("host"),
            "port": self.credentials.get("port", 3306),
            "user": self.credentials.get("username"),
            "password": self.credentials.get("password"),
            "database": self.credentials.get("database") or self.credentials.get("db"),
        }

        ssl_value = self.credentials.get("ssl", False)
        if isinstance(ssl_value, str):
            ssl_enabled = ssl_value.lower() in ('true', '1', 'yes')
        else:
            ssl_enabled = bool(ssl_value)
        conn_params["ssl_disabled"] = not ssl_enabled

        return mysql.connector.connect(**conn_params)
    
    async def test_connection(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "vendorVersion": "MySQL 8.0 (simulated)", "details": "Driver not available"}
        
        try:
            # Build connection parameters
            conn_params = {
                "host": self.credentials.get("host"),
                "port": self.credentials.get("port", 3306),
                "user": self.credentials.get("username"),
                "password": self.credentials.get("password")
            }
            
            # Handle SSL configuration properly for Azure and other cloud providers
            # credentials.ssl can be bool True/False or string 'true'/'false'
            ssl_value = self.credentials.get("ssl", False)
            if isinstance(ssl_value, str):
                ssl_enabled = ssl_value.lower() in ('true', '1', 'yes')
            else:
                ssl_enabled = bool(ssl_value)
            
            if ssl_enabled:
                # For Azure MySQL, SSL is required by default
                conn_params["ssl_disabled"] = False
            else:
                # Explicitly disable SSL for local/non-cloud MySQL
                conn_params["ssl_disabled"] = True
            
            # Test connection without specifying database (server-level connection)
            conn = mysql.connector.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {"ok": True, "vendorVersion": version, "details": "Connection successful"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    
    async def introspect_analysis(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "database_info": {"type": "MySQL", "version": "MySQL 8.0 (simulated)", "schemas": ["mydb"], "encoding": "utf8mb4", "collation": "utf8mb4_general_ci"},
                "tables": [{"schema": "mydb", "name": "customers", "type": "BASE TABLE", "row_count": 500, "engine": "InnoDB"}],
                "columns": [{"schema": "mydb", "table": "customers", "name": "id", "type": "int", "nullable": False, "default": None, "collation": None}],
                "constraints": [], "views": [], "procedures": [], "indexes": [], "triggers": [], "sequences": [],
                "user_types": [], "materialized_views": [], "partitions": [], "permissions": [],
                "data_profiles": [{"schema": "mydb", "table": "customers", "row_count": 500}],
                "driver_unavailable": True
            }
        
        try:
            # For MySQL, we'll discover all databases the user has access to
            # First, get all accessible databases
            
            # Build connection parameters
            conn_params = {
                "host": self.credentials.get("host"),
                "port": self.credentials.get("port", 3306),
                "user": self.credentials.get("username"),
                "password": self.credentials.get("password")
            }
            
            # Handle SSL configuration properly
            ssl_value = self.credentials.get("ssl", False)
            if isinstance(ssl_value, str):
                ssl_enabled = ssl_value.lower() in ('true', '1', 'yes')
            else:
                ssl_enabled = bool(ssl_value)
            
            conn_params["ssl_disabled"] = not ssl_enabled
            
            conn = mysql.connector.connect(**conn_params)
            cursor = conn.cursor(dictionary=True)
            
            # Get list of all databases (schemas) user has access to
            cursor.execute("SHOW DATABASES")
            all_databases = [row['Database'] for row in cursor.fetchall()]
            
            # Filter out system databases
            user_databases = [db for db in all_databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
            
            cursor.execute("SELECT VERSION() as version")
            version_info = cursor.fetchone()
            version = version_info['version'] if version_info else 'Unknown'
            
            # Get default encoding/collation (from first user database or server default)
            if user_databases:
                cursor.execute("""
                    SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME 
                    FROM information_schema.SCHEMATA 
                    WHERE SCHEMA_NAME = %s
                """, (user_databases[0],))
                db_info = cursor.fetchone()
                encoding = db_info['DEFAULT_CHARACTER_SET_NAME'] if db_info else 'utf8mb4'
                collation = db_info['DEFAULT_COLLATION_NAME'] if db_info else 'utf8mb4_general_ci'
            else:
                encoding = 'utf8mb4'
                collation = 'utf8mb4_general_ci'
            
            # Query tables from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, DATA_LENGTH, INDEX_LENGTH
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA IN ({db_list})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
            else:
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, DATA_LENGTH, INDEX_LENGTH
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
            tables_raw = cursor.fetchall()
            
            tables = []
            views = []
            total_data_size = 0
            total_index_size = 0
            
            # Get accurate row counts by executing COUNT(*) queries for each table
            for t in tables_raw:
                row_count = 0
                if t['TABLE_TYPE'] == 'BASE TABLE':
                    try:
                        count_cursor = conn.cursor()
                        count_cursor.execute(f"SELECT COUNT(*) FROM `{t['TABLE_SCHEMA']}`.`{t['TABLE_NAME']}`")
                        row_count = count_cursor.fetchone()[0]
                        count_cursor.close()
                    except Exception as e:
                        print(f"Error getting row count for {t['TABLE_SCHEMA']}.{t['TABLE_NAME']}: {e}")
                        row_count = 0
                
                item = {
                    "schema": t['TABLE_SCHEMA'],
                    "name": t['TABLE_NAME'],
                    "type": t['TABLE_TYPE'],
                    "row_count": row_count,
                    "data_length": t['DATA_LENGTH'] or 0,
                    "index_length": t['INDEX_LENGTH'] or 0
                }
                if t['TABLE_TYPE'] == 'BASE TABLE':
                    item["engine"] = t['ENGINE']
                    tables.append(item)
                    total_data_size += t['DATA_LENGTH'] or 0
                    total_index_size += t['INDEX_LENGTH'] or 0
                elif t['TABLE_TYPE'] == 'VIEW':
                    views.append(item)
            
            # Get database size information across all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT 
                        SUM(data_length) AS data_size,
                        SUM(index_length) AS index_size,
                        SUM(data_length + index_length) AS total_size
                    FROM information_schema.tables 
                    WHERE table_schema IN ({db_list})
                """)
            else:
                cursor.execute("""
                    SELECT 
                        SUM(data_length) AS data_size,
                        SUM(index_length) AS index_size,
                        SUM(data_length + index_length) AS total_size
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                """)
            
            db_size_info = cursor.fetchone()
            database_size = {
                "data_size": db_size_info['data_size'] if db_size_info and db_size_info['data_size'] else 0,
                "index_size": db_size_info['index_size'] if db_size_info and db_size_info['index_size'] else 0,
                "total_size": db_size_info['total_size'] if db_size_info and db_size_info['total_size'] else 0
            }
            
            # Get columns from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
                           IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE, COLLATION_NAME,
                           EXTRA, COLUMN_COMMENT
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA IN ({db_list})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
            else:
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
                           IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE, COLLATION_NAME,
                           EXTRA, COLUMN_COMMENT
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
            columns = []
            for c in cursor.fetchall():
                columns.append({
                    "schema": c['TABLE_SCHEMA'],
                    "table": c['TABLE_NAME'],
                    "name": c['COLUMN_NAME'],
                    "type": c['COLUMN_TYPE'],
                    "nullable": c['IS_NULLABLE'] == 'YES',
                    "default": c['COLUMN_DEFAULT'],
                    "collation": c['COLLATION_NAME'],
                    "extra": c['EXTRA'],
                    "comment": c['COLUMN_COMMENT']
                })
            
            # Get constraints from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                    FROM information_schema.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA IN ({db_list})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
                """)
            else:
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                    FROM information_schema.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
                """)
            constraints = []
            for c in cursor.fetchall():
                constraints.append({
                    "schema": c['TABLE_SCHEMA'],
                    "table": c['TABLE_NAME'],
                    "name": c['CONSTRAINT_NAME'],
                    "type": c['CONSTRAINT_TYPE']
                })
            
            # Get indexes from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, COLUMN_NAME, 
                           NON_UNIQUE, SEQ_IN_INDEX, INDEX_TYPE
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA IN ({db_list})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                """)
            else:
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, COLUMN_NAME, 
                           NON_UNIQUE, SEQ_IN_INDEX, INDEX_TYPE
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                """)
            indexes = []
            for i in cursor.fetchall():
                indexes.append({
                    "schema": i['TABLE_SCHEMA'],
                    "table": i['TABLE_NAME'],
                    "name": i['INDEX_NAME'],
                    "column": i['COLUMN_NAME'],
                    "unique": i['NON_UNIQUE'] == 0,
                    "type": i['INDEX_TYPE']
                })
            
            # Get triggers from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_MANIPULATION, 
                           EVENT_OBJECT_TABLE, ACTION_TIMING, ACTION_STATEMENT
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA IN ({db_list})
                    ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
                """)
            else:
                cursor.execute("""
                    SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_MANIPULATION, 
                           EVENT_OBJECT_TABLE, ACTION_TIMING, ACTION_STATEMENT
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
                """)
            triggers = []
            for t in cursor.fetchall():
                triggers.append({
                    "schema": t['TRIGGER_SCHEMA'],
                    "name": t['TRIGGER_NAME'],
                    "table": t['EVENT_OBJECT_TABLE'],
                    "timing": t['ACTION_TIMING'],
                    "event": t['EVENT_MANIPULATION'],
                    "definition": t['ACTION_STATEMENT']
                })
            
            # Get procedures from all user databases
            if user_databases:
                db_list = ','.join([f"'{db}'" for db in user_databases])
                cursor.execute(f"""
                    SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA IN ({db_list})
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
                """)
            else:
                cursor.execute("""
                    SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
                """)
            procedures = []
            for p in cursor.fetchall():
                procedures.append({
                    "schema": p['ROUTINE_SCHEMA'],
                    "name": p['ROUTINE_NAME'],
                    "type": p['ROUTINE_TYPE']
                })
            
            cursor.close()
            conn.close()
            
            return {
                "database_info": {
                    "type": "MySQL",
                    "version": version,
                    "schemas": user_databases,  # Return all user databases as schemas
                    "encoding": encoding,
                    "collation": collation
                },
                "tables": tables,
                "columns": columns,
                "constraints": constraints,
                "views": views,
                "procedures": procedures,
                "indexes": indexes,
                "triggers": triggers,
                "sequences": [],
                "user_types": [],
                "materialized_views": [],
                "partitions": [],
                "permissions": [],
                "data_profiles": [{"schema": t['schema'], "table": t['name'], "row_count": t['row_count']} for t in tables],
                "storage_info": {
                    "database_size": database_size,
                    "tables": [{"schema": t['schema'], "name": t['name'], "data_length": t['data_length'], "index_length": t['index_length']} for t in tables]
                }
            }
        except Exception as e:
            return {
                "database_info": {"type": "MySQL", "version": f"Error: {str(e)}", "schemas": [], "encoding": "unknown", "collation": "unknown"},
                "tables": [], "columns": [], "constraints": [], "views": [], "procedures": [], 
                "indexes": [], "triggers": [], "sequences": [], "user_types": [],
                "materialized_views": [], "partitions": [], "permissions": [],
                "data_profiles": [],
                "error": str(e)
            }
    
    async def extract_objects(self, selected_tables: list = None) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ddl_scripts": {
                    "user_types": [],
                    "sequences": [],
                    "tables": [{"name": "customers", "schema": "mydb", "ddl": "CREATE TABLE customers (id INT PRIMARY KEY);"}],
                    "constraints": [],
                    "indexes": [],
                    "views": [],
                    "materialized_views": [],
                    "triggers": [],
                    "procedures": [],
                    "functions": [],
                    "grants": [],
                    "validation_scripts": []
                },
                "object_count": 1,
                "extraction_summary": {"user_types": 0, "sequences": 0, "tables": 1, "constraints": 0, "indexes": 0, "views": 0, "materialized_views": 0, "triggers": 0, "procedures": 0, "functions": 0, "grants": 0, "validation_scripts": 0},
                "driver_unavailable": True
            }
        
        try:
            # Build connection parameters
            conn_params = {
                "host": self.credentials.get("host"),
                "port": self.credentials.get("port", 3306),
                "database": self.credentials.get("database"),
                "user": self.credentials.get("username"),
                "password": self.credentials.get("password")
            }

            # Handle SSL configuration
            ssl_value = self.credentials.get("ssl", False)
            if isinstance(ssl_value, str):
                ssl_enabled = ssl_value.lower() in ('true', '1', 'yes')
            else:
                ssl_enabled = bool(ssl_value)

            conn_params["ssl_disabled"] = not ssl_enabled

            selected_tables = selected_tables or []
            # Build schema(database)->tables mapping purely from user selection.
            # This avoids scanning all schemas/tables when selection is provided.
            db_to_tables: Dict[str, set[str]] = {}
            for raw in selected_tables:
                item = str(raw or "").strip().replace("`", "")
                if not item:
                    continue
                if "." in item:
                    db, table = item.split(".", 1)
                    db = db.strip()
                    table = table.strip()
                else:
                    db = str(self.credentials.get("database") or "").strip()
                    table = item
                if not db:
                    raise Exception("MySQL extraction: database is required when selecting tables across schemas.")
                if not table:
                    continue
                if db not in db_to_tables:
                    db_to_tables[db] = set()
                db_to_tables[db].add(table)

            extracted_scripts = {
                "user_types": [],
                "sequences": [],
                "tables": [],
                "constraints": [],
                "indexes": [],
                "views": [],
                "materialized_views": [],
                "triggers": [],
                "procedures": [],
                "functions": [],
                "grants": [],
                "validation_scripts": []
            }

            # If no selection was provided, fall back to scanning only the configured database (legacy behavior).
            if not db_to_tables:
                db_name = str(self.credentials.get("database") or "").strip()
                if not db_name:
                    raise Exception("MySQL extraction: no database specified. Please set the database name in the connection.")

                conn = mysql.connector.connect(**conn_params)
                cursor = conn.cursor(dictionary=True)
                cursor.execute(f"SHOW TABLES FROM {self._quote_identifier(db_name)}")
                tables = [list(row.values())[0] for row in cursor.fetchall()]
                if not tables:
                    raise Exception(f"No tables found in MySQL database '{db_name}'. Verify database name and privileges.")

                for table in tables:
                    cursor.execute(f"SHOW CREATE TABLE {self._quote_identifier(db_name)}.{self._quote_identifier(table)}")
                    result = cursor.fetchone()
                    create_ddl = list(result.values())[1]
                    create_ddl = self._strip_db_qualifier_from_create_table(create_ddl, db_name, table)
                    extracted_scripts["tables"].append({"name": table, "schema": db_name, "ddl": create_ddl + ";"})
                    extracted_scripts["validation_scripts"].append({
                        "schema": db_name,
                        "table": table,
                        "sql": f"-- Validate row count for {db_name}.{table}\nSELECT COUNT(*) FROM {db_name}.{table};"
                    })

                cursor.close()
                conn.close()
            else:
                # Selection-driven extraction (MySQL -> Snowflake reference behavior):
                # iterate only user-selected schemas, and within them only selected tables.
                conn = mysql.connector.connect(**conn_params)
                cursor = conn.cursor(dictionary=True)
                errors: List[str] = []

                for db_name in sorted(db_to_tables.keys(), key=lambda s: s.lower()):
                    for table in sorted(db_to_tables[db_name], key=lambda s: s.lower()):
                        try:
                            cursor.execute(
                                f"SHOW CREATE TABLE {self._quote_identifier(db_name)}.{self._quote_identifier(table)}"
                            )
                            result = cursor.fetchone()
                            if not result:
                                raise Exception("SHOW CREATE TABLE returned no rows")
                            create_ddl = list(result.values())[1]
                            create_ddl = self._strip_db_qualifier_from_create_table(create_ddl, db_name, table)
                            extracted_scripts["tables"].append({"name": table, "schema": db_name, "ddl": create_ddl + ";"})
                            extracted_scripts["validation_scripts"].append({
                                "schema": db_name,
                                "table": table,
                                "sql": f"-- Validate row count for {db_name}.{table}\nSELECT COUNT(*) FROM {db_name}.{table};"
                            })
                        except Exception as e:
                            errors.append(f"{db_name}.{table}: {str(e)}")

                cursor.close()
                conn.close()

                if errors:
                    raise Exception("MySQL extraction failed for some selected tables: " + "; ".join(errors[:5]))
                if not extracted_scripts["tables"]:
                    raise Exception("No selected tables could be extracted. Verify selection, database names, and permissions.")

            # Keep extraction summary minimal and consistent.
            extraction_summary = {
                "user_types": 0,
                "sequences": 0,
                "tables": len(extracted_scripts["tables"]),
                "constraints": 0,
                "indexes": 0,
                "views": 0,
                "materialized_views": 0,
                "triggers": 0,
                "procedures": 0,
                "functions": 0,
                "grants": 0,
                "validation_scripts": len(extracted_scripts["validation_scripts"])
            }
            object_count = sum(extraction_summary.values())

            return {
                "ddl_scripts": extracted_scripts,
                "object_count": object_count,
                "extraction_summary": extraction_summary,
                "driver_unavailable": False
            }

        except Exception as e:
            empty_scripts = {
                "user_types": [],
                "sequences": [],
                "tables": [],
                "constraints": [],
                "indexes": [],
                "views": [],
                "materialized_views": [],
                "triggers": [],
                "procedures": [],
                "functions": [],
                "grants": [],
                "validation_scripts": []
            }
            return {
                "ddl_scripts": empty_scripts,
                "object_count": 0,
                "extraction_summary": {
                    "user_types": 0,
                    "sequences": 0,
                    "tables": 0,
                    "constraints": 0,
                    "indexes": 0,
                    "views": 0,
                    "materialized_views": 0,
                    "triggers": 0,
                    "procedures": 0,
                    "functions": 0,
                    "grants": 0,
                    "validation_scripts": 0
                },
                "driver_unavailable": False,
                "error": str(e)
            }
    
    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": True,
                "created": len(translated_ddl),
                "driver_unavailable": True,
                "message": f"Simulated: would create {len(translated_ddl)} objects"
            }
        
        import mysql.connector
        
        results = []
        for obj in translated_ddl:
            try:
                print(f"[MySQL] Creating object: {obj.get('name')}")
                print(f"[MySQL] SQL: {obj.get('target_sql', 'NO SQL PROVIDED')[:200]}")
                
                conn = mysql.connector.connect(
                    host=self.credentials.get("host"),
                    port=self.credentials.get("port", 3306),
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    database=self.credentials.get("database")
                )
                cur = conn.cursor()
                
                # Drop table if it exists to ensure clean migration
                cur.execute(f'DROP TABLE IF EXISTS `{obj["name"]}`')
                
                # Create the new table
                target_sql = obj.get("target_sql")
                if not target_sql:
                    raise Exception("No target_sql provided")
                    
                cur.execute(target_sql)
                conn.commit()
                cur.close()
                conn.close()
                print(f"[MySQL] Successfully created {obj.get('name')}")
                results.append({"name": obj["name"], "status": "success"})
            except Exception as e:
                print(f"[MySQL] ERROR creating {obj.get('name')}: {str(e)}")
                results.append({"name": obj["name"], "status": "error", "error": str(e)})
        
        return {"ok": True, "results": results}

    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "dropped": len(table_names)}

        import mysql.connector

        def _q(ident: str) -> str:
            return "`" + str(ident).replace("`", "``") + "`"

        dropped = 0
        errors: List[Dict[str, Any]] = []

        for ref in table_names or []:
            try:
                raw = str(ref or "").strip()
                if not raw:
                    continue
                parts = [p for p in raw.split(".") if p]
                if len(parts) >= 2:
                    db_name, table_name = parts[-2], parts[-1]
                else:
                    db_name, table_name = self.credentials.get("database"), parts[0]
                if not table_name:
                    continue

                conn = mysql.connector.connect(
                    host=self.credentials.get("host"),
                    port=self.credentials.get("port", 3306),
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    database=db_name or self.credentials.get("database")
                )
                cur = conn.cursor()
                if db_name:
                    cur.execute(f"USE {_q(db_name)}")
                cur.execute(f"DROP TABLE IF EXISTS {_q(table_name)}")
                conn.commit()
                cur.close()
                conn.close()
                dropped += 1
            except Exception as e:
                errors.append({"table": ref, "error": str(e)})

        return {"ok": len(errors) == 0, "dropped": dropped, "errors": errors}
    
    async def yield_table_data(self, table_name: str, chunk_size: int = 10000, columns: Optional[List[str]] = None):
        """Async generator to yield data from MySQL table in chunks as (columns, rows) tuples"""
        if not self.driver_available:
            return
        
        try:
            import mysql.connector
            
            # Choose database: explicit credential or schema prefix from table_name
            db_name = self.credentials.get("database")
            if (not db_name) and "." in table_name:
                db_name = table_name.split(".", 1)[0]
            if not db_name:
                raise Exception("MySQL yield_table_data: no database selected. Set database in connection.")

            conn = mysql.connector.connect(
                host=self.credentials.get("host"),
                port=self.credentials.get("port", 3306),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                database=db_name
            )
            cur = conn.cursor()
            
            # Extract just the table name without schema prefix
            target_table = table_name.split('.')[-1] if '.' in table_name else table_name
            
            requested_columns = [str(c) for c in (columns or []) if str(c or "").strip()]
            if requested_columns:
                cols_sql = ", ".join(f"`{col}`" for col in requested_columns)
                cur.execute(f"SELECT {cols_sql} FROM `{target_table}`")
            else:
                # Query all data from table
                cur.execute(f"SELECT * FROM `{target_table}`")
            
            # Get column names
            columns = [desc[0] for desc in cur.description]
            
            # Yield data in chunks
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield (columns, rows)
            
            cur.close()
            conn.close()
            
        except Exception as e:
            raise Exception(f"Error reading MySQL table {table_name}: {str(e)}")
    
    async def copy_table_data(
        self,
        table_name: str,
        source_adapter: 'DatabaseAdapter',
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "table": table_name, "rows_copied": 500, "driver_unavailable": True, "status": "Success"}
        
        try:
            # Check if source adapter has the yield_table_data method
            if not hasattr(source_adapter, 'yield_table_data'):
                return {
                    "ok": False,
                    "error": f"Source adapter does not support data streaming",
                    "rows_copied": 0,
                    "status": "Error"
                }
            
            import mysql.connector
            
            # Connect to MySQL target
            target_conn = mysql.connector.connect(
                host=self.credentials.get("host"),
                port=self.credentials.get("port", 3306),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                database=self.credentials.get("database")
            )
            target_cur = target_conn.cursor()
            
            rows_inserted = 0
            insert_sql = None
            
            # Stream data from source in chunks
            async for columns, rows in source_adapter.yield_table_data(table_name, chunk_size, columns=columns):
                if not rows:
                    continue
                
                # Build INSERT statement on first chunk
                if insert_sql is None:
                    # Extract just the table name without schema prefix
                    target_table = table_name.split('.')[-1] if '.' in table_name else table_name
                    
                    columns_str = ', '.join([f'`{col}`' for col in columns])
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f'INSERT INTO `{target_table}` ({columns_str}) VALUES ({placeholders})'
                
                # Insert this chunk
                target_cur.executemany(insert_sql, rows)
                target_conn.commit()
                rows_inserted += len(rows)
                if callable(progress_cb):
                    try:
                        progress_cb(rows_inserted, len(rows))
                    except Exception:
                        pass
            
            target_cur.close()
            target_conn.close()
            
            return {
                "ok": True,
                "table": table_name,
                "rows_copied": rows_inserted,
                "status": "Success"
            }
        except Exception as e:
            return {
                "ok": False,
                "table": table_name,
                "rows_copied": 0,
                "status": "Error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }
    
    async def run_validation_checks(self, source_adapter: 'DatabaseAdapter', table_names: List[str] | None = None) -> Dict[str, Any]:
        # This method is called by Snowflake adapter when validating MySQL -> Snowflake migrations
        # The actual validation is done in the Snowflake adapter's method
        # Return a basic response since validation happens in target adapter
        return {"structural": {"schema_match": True}, "data": {"row_counts_match": True}, "driver_unavailable": False}
    
    async def get_table_row_count(self, table_name: str) -> int:
        if not self.driver_available:
            return 1000
        
        try:
            import mysql.connector
            db_name = self.credentials.get("database") or self.credentials.get("db")
            if (not db_name) and "." in table_name:
                db_name = table_name.split(".", 1)[0]
            if not db_name:
                raise Exception("MySQL row count: no database specified.")
            conn = mysql.connector.connect(
                host=self.credentials.get("host"),
                port=self.credentials.get("port", 3306),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                database=db_name
            )
            cur = conn.cursor()
            # Ensure DB context is set
            cur.execute(f"USE `{db_name.replace('`','``')}`")
            
            # Use parameterized query to prevent SQL injection
            # If table_name includes schema, strip it after using it to set db
            target_table = table_name.split(".")[-1] if "." in table_name else table_name
            query = f"SELECT COUNT(*) FROM `{db_name.replace('`','``')}`.`{target_table.replace('`','``')}`"
            cur.execute(query)
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count
        except Exception as e:
            print(f"Error getting row count for {table_name}: {e}")
            raise
    
    async def get_schema_structure(self, tables_ddl: list) -> dict:
        if not self.driver_available:
            return {}
        
        schema_info = {}
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=self.credentials.get("host"),
                port=self.credentials.get("port", 3306),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                database=self.credentials.get("database")
            )
            cur = conn.cursor()
            
            for table in tables_ddl:
                table_name = table.get("name", "")
                # For MySQL target, always use the target database name, not the source schema
                target_database = self.credentials.get("database")
                
                # Use parameterized query to prevent SQL injection
                cur.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
                    ORDER BY ORDINAL_POSITION
                """, (table_name, target_database))
                columns = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
                print(f"[MySQL] Table {table_name} has {len(columns)} columns: {[c['name'] for c in columns]}")
                schema_info[table_name] = columns
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error getting schema structure: {e}")
            raise
        
        return schema_info

    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        """Rename a column in a MySQL table using ALTER TABLE ... CHANGE COLUMN."""
        if not self.driver_available:
            return {"ok": False, "message": "MySQL driver not available"}
        
        def _qident(identifier: str) -> str:
            return '`' + str(identifier).replace('`', '``') + '`'
        
        def _clean_ident(identifier: str) -> str:
            return str(identifier).strip().strip('`')
        
        try:
            import mysql.connector
            
            # Parse table name to extract database and table parts
            if "." in table_name:
                db_name, table_part = table_name.split(".", 1)
                db_name = _clean_ident(db_name)
                table_part = _clean_ident(table_part)
            else:
                db_name = self.credentials.get("database") or self.credentials.get("db")
                table_part = _clean_ident(table_name)
            
            if not db_name:
                return {"ok": False, "message": "Database name not provided and not available in credentials"}
            
            # Clean column names
            old_col_clean = _clean_ident(old_column_name)
            new_col_clean = _clean_ident(new_column_name)
            
            # Connect to MySQL
            conn = mysql.connector.connect(
                host=self.credentials.get("host"),
                port=self.credentials.get("port", 3306),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
                database=db_name
            )
            cursor = conn.cursor()
            
            try:
                # Get the current column definition to preserve it during change
                cursor.execute(f"SHOW COLUMNS FROM {_qident(table_part)} LIKE '{old_col_clean}'")
                result = cursor.fetchone()
                
                if not result:
                    return {"ok": False, "message": f"Column '{old_col_clean}' does not exist in table {table_part}"}
                
                # Extract column definition details
                current_field = result[0]  # Field name
                current_type = result[1]  # Type
                current_null = result[2]  # Null (YES/NO)
                current_key = result[3]   # Key
                current_default = result[4]  # Default
                current_extra = result[5]    # Extra
                
                # Build the new column definition
                # Preserve all attributes of the old column
                new_col_def = f"{_qident(new_col_clean)} {current_type}"
                
                if current_null == "NO":
                    new_col_def += " NOT NULL"
                
                if current_default is not None:
                    if current_default == "NULL":
                        new_col_def += " DEFAULT NULL"
                    else:
                        # Properly quote the default value
                        new_col_def += f" DEFAULT '{current_default}'" if current_default != "" else " DEFAULT ''"
                
                if current_extra:
                    new_col_def += f" {current_extra}"
                
                # Execute ALTER TABLE CHANGE COLUMN
                alter_sql = f"ALTER TABLE {_qident(table_part)} CHANGE COLUMN {_qident(old_col_clean)} {new_col_def}"
                cursor.execute(alter_sql)
                
                conn.commit()
                return {"ok": True, "message": f"Successfully renamed column {old_col_clean} to {new_col_clean} in {db_name}.{table_part}"}
            except Exception as e:
                conn.rollback()
                return {"ok": False, "message": f"Failed to rename column: {str(e)}"}
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            return {"ok": False, "message": f"Error connecting to MySQL: {str(e)}"}
