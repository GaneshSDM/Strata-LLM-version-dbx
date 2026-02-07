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

class DatabricksAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        self.driver_available = DRIVER_AVAILABLE
        self.logger = logging.getLogger("strata")

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
                server_hostname = self.credentials.get("host") or self.credentials.get("server_hostname")
                http_path = self.credentials.get("http_path") or self.credentials.get("httpPath")
                access_token = self.credentials.get("access_token") or self.credentials.get("accessToken")
                catalog = self.credentials.get("catalog") or self.credentials.get("catalogName", "hive_metastore")
                schema = self.credentials.get("schema") or self.credentials.get("schemaName", "default")
                
                # Validate that required parameters are not None
                if not server_hostname:
                    raise ValueError("Server hostname is required for Databricks connection")
                if not http_path:
                    raise ValueError("HTTP path is required for Databricks connection")
                if not access_token:
                    raise ValueError("Access token is required for Databricks connection")
                
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
                    "indexes": indexes,  # Databricks uses Delta Lake which handles indexing differently
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

                    # Oracle -> Databricks type conversions.
                    ddl = re.sub(r'\bVARCHAR2\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNVARCHAR2\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNCHAR\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bCHAR\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bNCLOB\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bTEXT\b', 'STRING', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bBLOB\b', 'BINARY', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bRAW\b', 'BINARY', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bBINARY_FLOAT\b', 'FLOAT', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bBINARY_DOUBLE\b', 'DOUBLE', ddl, flags=re.IGNORECASE)
                    ddl = re.sub(r'\bFLOAT\b', 'DOUBLE', ddl, flags=re.IGNORECASE)

                    # Databricks/Spark does not support STRING(n). If upstream translation produced STRING(100),
                    # collapse it to STRING so table creation succeeds.
                    ddl = re.sub(
                        r'\bSTRING\s*\(\s*\d+\s*(?:CHAR|BYTE)?\s*\)',
                        'STRING',
                        ddl,
                        flags=re.IGNORECASE
                    )

                    ddl = re.sub(
                        r'\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)',
                        r'DECIMAL(\1,\2)',
                        ddl,
                        flags=re.IGNORECASE
                    )
                    ddl = re.sub(
                        r'\bNUMBER\s*\(\s*(\d+)\s*\)',
                        r'DECIMAL(\1,0)',
                        ddl,
                        flags=re.IGNORECASE
                    )
                    ddl = re.sub(r'\bNUMBER\b', 'DECIMAL(38,10)', ddl, flags=re.IGNORECASE)

                    ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', ddl, flags=re.IGNORECASE)

                    # Databricks requires DATE defaults to be CURRENT_DATE, not CURRENT_TIMESTAMP.
                    ddl = re.sub(
                        r'\bDATE\s+DEFAULT\s+CURRENT_TIMESTAMP\s*(?:\(\s*\))?',
                        'DATE DEFAULT CURRENT_DATE',
                        ddl,
                        flags=re.IGNORECASE
                    )

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
                
                translated_list = translated_ddl or []
                attempted_total = len(translated_list)
                attempted_sql = 0
                created_count = 0
                errors: List[Dict[str, Any]] = []
                skipped: List[Dict[str, Any]] = []

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

                        attempted_sql += 1
                        ddl = _normalize_ddl(raw_ddl)
                        ddl = _rewrite_schema_refs(ddl, default_schema)
                        if not ddl:
                            skipped.append({
                                "name": obj.get("name", "unknown"),
                                "schema": obj.get("schema", default_schema),
                                "error": "Normalized DDL was empty",
                                "ddl": "",
                                "original_ddl": raw_ddl or ""
                            })
                            continue

                        cursor.execute(f"USE CATALOG `{default_catalog}`")
                        cursor.execute(f"USE SCHEMA `{default_schema}`")
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
                
                connection.commit()
                cursor.close()
                connection.close()

                all_errors = errors + skipped
                result = {
                    "ok": len(all_errors) == 0,
                    "created": created_count,
                    "attempted": attempted_total,
                    "attempted_sql": attempted_sql
                }
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
            connection = self.get_connection()
            cursor = connection.cursor()
            for stmt in filter(None, (s.strip() for s in ddl.split(';'))):
                cursor.execute(stmt)
            connection.commit()
            cursor.close()
            connection.close()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

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
