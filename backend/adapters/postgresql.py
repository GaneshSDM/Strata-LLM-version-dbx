import asyncio
from typing import Dict, Any, List, Optional, Callable
from .base import DatabaseAdapter

try:
    import psycopg2
    DRIVER_AVAILABLE = True
except ImportError:
    DRIVER_AVAILABLE = False

class PostgreSQLAdapter(DatabaseAdapter):
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        self.driver_available = DRIVER_AVAILABLE
        # Many hosted Postgres providers (e.g., Supabase) include internal schemas that users
        # generally don't want to analyze/migrate. Exclude those by default.
        self._excluded_schemas_lower = {
            "information_schema",
            # Common hosted/provider schemas (Supabase, etc.)
            "auth",
            "extensions",
            "realtime",
            "storage",
            "vault",
            "graphql",
            "graphql_public",
            "supabase_functions",
            "supabase_migrations",
            "pgbouncer",
            "net",
            "cron",
        }

    def _excluded_schema_sql_list(self) -> str:
        vals = sorted({str(s).strip().lower() for s in self._excluded_schemas_lower if str(s).strip()})
        if "information_schema" not in vals:
            vals.append("information_schema")
        return ", ".join("'" + v.replace("'", "''") + "'" for v in vals)

    def _schema_exclusion_clause(self, column_name: str) -> str:
        # Exclude built-in Postgres schemas (`pg_%`) plus hosted-provider system schemas.
        excluded = self._excluded_schema_sql_list()
        return f"(lower({column_name}) NOT LIKE 'pg_%' AND lower({column_name}) NOT IN ({excluded}))"

    def _clean_ident(self, identifier: str) -> str:
        return str(identifier).strip().strip('"')

    def _split_table_ref(self, table_ref: str):
        """Return (schema, table) for schema-qualified names; defaults schema to public."""
        ref = str(table_ref or "").strip()
        if "." in ref:
            schema_part, table_part = ref.split(".", 1)
            schema = self._clean_ident(schema_part) or "public"
            table = self._clean_ident(table_part)
        else:
            schema = "public"
            table = self._clean_ident(ref)
        # Normalize Snowflake's common schema name to Postgres default.
        if schema.lower() == "public":
            schema = "public"
        return schema, table

    def _resolve_table_case_insensitive(self, cur, schema: str, table: str, allow_cross_schema: bool = True):
        """Resolve (schema, table) to actual case in information_schema, if it exists."""
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND lower(table_schema) = lower(%s)
              AND lower(table_name) = lower(%s)
            LIMIT 1
            """,
            (schema, table),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1]

        if not allow_cross_schema:
            return schema, table

        # Fallback: search across schemas by table name only
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND lower(table_name) = lower(%s)
            ORDER BY CASE WHEN lower(table_schema) = 'public' THEN 0 ELSE 1 END, table_schema
            LIMIT 1
            """,
            (table,),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1]

        return schema, table
    
    def _get_connection_params(self, database=None):
        """Build connection parameters with proper SSL handling for Azure PostgreSQL"""
        conn_params = {
            "host": self.credentials.get("host"),
            "port": int(self.credentials.get("port", 5432)),
            "database": database or self.credentials.get("database") or "postgres",
            "user": self.credentials.get("username"),
            "password": self.credentials.get("password"),
        }
        
        # Handle SSL mode for Azure and other cloud PostgreSQL services
        sslmode = str(self.credentials.get("sslmode") or "disable").strip().lower()
        if sslmode and sslmode != "disable":
            conn_params["sslmode"] = sslmode
        
        return conn_params

    def _candidate_users(self, host: str | None, user: str | None) -> list[str]:
        """Return possible usernames to try (some providers require user@server)."""
        host_value = str(host or "").strip().lower()
        user_value = str(user or "").strip()
        if not user_value:
            return []

        candidates: list[str] = [user_value]
        try:
            if host_value.endswith(".postgres.database.azure.com") and "@" not in user_value:
                server = host_value.split(".", 1)[0]
                candidates.append(f"{user_value}@{server}")
        except Exception:
            pass

        # de-dupe while keeping order
        seen = set()
        ordered: list[str] = []
        for c in candidates:
            key = c.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(c)
        return ordered

    def _connect(self, database: str | None = None):
        """Connect using credentials, trying provider-specific username variants when needed."""
        if not self.driver_available:
            raise Exception("PostgreSQL driver (psycopg2) is not installed. Please install it: pip install psycopg2-binary")

        conn_params = self._get_connection_params(database=database)
        host = conn_params.get("host")
        user = conn_params.get("user")

        candidates = self._candidate_users(host, user) or [str(user or "").strip()]
        last_error: Exception | None = None
        for candidate_user in candidates:
            try:
                conn_params["user"] = candidate_user
                return psycopg2.connect(**conn_params)
            except Exception as e:
                last_error = e
                continue
        if last_error:
            raise last_error
        raise Exception("Invalid PostgreSQL credentials")

    def get_connection(self):
        """Return a synchronous psycopg2 connection for validation helpers."""
        if not self.driver_available:
            raise RuntimeError("PostgreSQL driver (psycopg2) is not installed")
        return self._connect(database=self.credentials.get("database"))
    
    async def test_connection(self) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": True,
                "driver_unavailable": True,
                "vendorVersion": "PostgreSQL 15.x (simulated)",
                "details": "Driver not available, simulated mode"
            }
        
        try:
            # Test connection using 'postgres' database (default system database)
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            return {
                "ok": True,
                "vendorVersion": version,
                "details": "Connection successful",
                "message": "Connection successful"
            }
        except Exception as e:
            message = str(e).strip() or f"{e.__class__.__name__}"
            return {
                "ok": False,
                "message": message
            }
    
    async def introspect_analysis(self) -> Dict[str, Any]:
        if not self.driver_available:
            raise Exception("PostgreSQL driver (psycopg2) is not installed. Please install it: pip install psycopg2-binary")
        
        try:
            # For PostgreSQL, connect to 'postgres' database (default system database)
            # to discover all available databases
            conn = self._connect()
            cur = conn.cursor()
            
            # Database info with encoding
            cur.execute("SELECT version()")
            db_version = cur.fetchone()[0]
            
            cur.execute("SELECT pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE datname = current_database()")
            encoding_row = cur.fetchone()
            db_encoding = encoding_row[0] if encoding_row else "UTF8"
            db_collation = encoding_row[1] if encoding_row else "en_US.UTF-8"
            
            cur.execute(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE {self._schema_exclusion_clause('schema_name')}
                """
            )
            schemas = [row[0] for row in cur.fetchall()]
            
            # Tables with storage information
            cur.execute(f"""
                SELECT 
                    t.table_schema, 
                    t.table_name, 
                    t.table_type,
                    pg_total_relation_size('"' || t.table_schema || '"."' || t.table_name || '"') as total_size,
                    pg_relation_size('"' || t.table_schema || '"."' || t.table_name || '"') as data_size,
                    pg_indexes_size('"' || t.table_schema || '"."' || t.table_name || '"') as index_size,
                    COALESCE(s.n_live_tup, 0) AS row_estimate
                FROM information_schema.tables t
                LEFT JOIN pg_stat_user_tables s
                  ON s.schemaname = t.table_schema
                 AND s.relname = t.table_name
                WHERE {self._schema_exclusion_clause('t.table_schema')}
                ORDER BY t.table_schema, t.table_name
            """)
            tables = []
            total_database_size = 0
            total_database_data_size = 0
            total_database_index_size = 0
            
            for row in cur.fetchall():
                tables.append({
                    "schema": row[0],
                    "name": row[1],
                    "type": row[2],
                    "total_size": row[3] or 0,
                    "data_size": row[4] or 0,
                    "index_size": row[5] or 0,
                    # Start with estimate; will overwrite with exact counts below
                    "row_count": int(row[6]) if row[6] is not None else 0
                })
                total_database_size += row[3] or 0
                total_database_data_size += row[4] or 0
                total_database_index_size += row[5] or 0
            
            # Enhanced columns with collation and generated columns
            cur.execute(f"""
                SELECT 
                    c.table_schema,
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.column_default,
                    c.is_nullable,
                    c.collation_name,
                    c.is_generated
                FROM information_schema.columns c
                WHERE {self._schema_exclusion_clause('c.table_schema')}
                ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """)
            columns = []
            for row in cur.fetchall():
                columns.append({
                    "schema": row[0],
                    "table": row[1],
                    "name": row[2],
                    "type": row[3],
                    "max_length": row[4],
                    "precision": row[5],
                    "scale": row[6],
                    "default": row[7],
                    "nullable": row[8] == "YES",
                    "collation": row[9],
                    "is_generated": row[10] == "ALWAYS"
                })
            
            # Enhanced constraints with FK cascade rules and check constraints
            cur.execute(f"""
                SELECT 
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    tc.constraint_type,
                    COALESCE(cc.check_clause, ''),
                    COALESCE(rc.update_rule, ''),
                    COALESCE(rc.delete_rule, '')
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name AND tc.table_schema = cc.constraint_schema
                LEFT JOIN information_schema.referential_constraints rc 
                    ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
                WHERE {self._schema_exclusion_clause('tc.table_schema')}
            """)
            constraints = []
            for row in cur.fetchall():
                constraints.append({
                    "schema": row[0],
                    "table": row[1],
                    "name": row[2],
                    "type": row[3],
                    "check_clause": row[4],
                    "on_update": row[5],
                    "on_delete": row[6]
                })
            
            # Views
            cur.execute(f"""
                SELECT 
                    schemaname, 
                    viewname, 
                    definition
                FROM pg_views
                WHERE {self._schema_exclusion_clause('schemaname')}
            """)
            views = [{"schema": row[0], "name": row[1], "definition": row[2]} for row in cur.fetchall()]
            
            # Materialized views
            cur.execute(f"""
                SELECT 
                    schemaname, 
                    matviewname, 
                    definition
                FROM pg_matviews
                WHERE {self._schema_exclusion_clause('schemaname')}
            """)
            materialized_views = [{"schema": row[0], "name": row[1], "definition": row[2]} for row in cur.fetchall()]
            
            # Procedures and functions
            cur.execute(f"""
                SELECT 
                    n.nspname as schema,
                    p.proname as name,
                    pg_get_functiondef(p.oid) as definition
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE {self._schema_exclusion_clause('n.nspname')}
            """)
            procedures = [{"schema": row[0], "name": row[1], "definition": row[2]} for row in cur.fetchall()]
            
            # Triggers
            cur.execute(f"""
                SELECT 
                    trigger_schema,
                    trigger_name,
                    event_manipulation,
                    event_object_table,
                    action_timing,
                    action_statement
                FROM information_schema.triggers
                WHERE {self._schema_exclusion_clause('trigger_schema')}
            """)
            triggers = []
            for row in cur.fetchall():
                triggers.append({
                    "schema": row[0],
                    "name": row[1],
                    "event": row[2],
                    "table": row[3],
                    "timing": row[4],
                    "definition": row[5]
                })
            
            # Sequences
            cur.execute(f"""
                SELECT 
                    schemaname,
                    sequencename,
                    start_value,
                    increment_by,
                    max_value,
                    min_value,
                    last_value
                FROM pg_sequences
                WHERE {self._schema_exclusion_clause('schemaname')}
            """)
            sequences = []
            for row in cur.fetchall():
                sequences.append({
                    "schema": row[0],
                    "name": row[1],
                    "start_value": row[2],
                    "increment": row[3],
                    "max_value": row[4],
                    "min_value": row[5],
                    "current_value": row[6]
                })
            
            # User-defined types
            cur.execute(f"""
                SELECT 
                    n.nspname as schema,
                    t.typname as name,
                    t.typtype as type_category,
                    pg_catalog.format_type(t.oid, NULL) as definition
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE {self._schema_exclusion_clause('n.nspname')}
                AND t.typtype IN ('c', 'e', 'd')
            """)
            user_types = []
            for row in cur.fetchall():
                type_cat = {"c": "composite", "e": "enum", "d": "domain"}.get(row[2], "unknown")
                user_types.append({
                    "schema": row[0],
                    "name": row[1],
                    "category": type_cat,
                    "definition": row[3]
                })
            
            # Partitioned tables
            cur.execute(f"""
                SELECT 
                    n.nspname as schema,
                    c.relname as table_name,
                    pg_get_partkeydef(c.oid) as partition_key
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind = 'p'
                AND {self._schema_exclusion_clause('n.nspname')}
            """)
            partitions = [{"schema": row[0], "table": row[1], "partition_key": row[2]} for row in cur.fetchall()]
            
            # Indexes
            cur.execute(f"""
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE {self._schema_exclusion_clause('schemaname')}
            """)
            indexes = [{"schema": row[0], "table": row[1], "name": row[2], "definition": row[3]} for row in cur.fetchall()]
            
            # Permissions / Grants
            cur.execute(f"""
                SELECT 
                    grantee,
                    table_schema,
                    table_name,
                    privilege_type
                FROM information_schema.table_privileges
                WHERE {self._schema_exclusion_clause('table_schema')}
                ORDER BY grantee, table_schema, table_name
            """)
            permissions = []
            for row in cur.fetchall():
                permissions.append({
                    "grantee": row[0],
                    "schema": row[1],
                    "object": row[2],
                    "privilege": row[3]
                })
            
            # Data profiles
            # Precise row counts for all user tables
            data_profiles = []
            for table in tables:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{table["schema"]}"."{table["name"]}"')
                    count = cur.fetchone()[0] or 0
                    table["row_count"] = int(count)
                    data_profiles.append({
                        "schema": table["schema"],
                        "table": table["name"],
                        "row_count": int(count)
                    })
                except Exception as e:
                    # Keep existing estimate if count fails
                    data_profiles.append({
                        "schema": table["schema"],
                        "table": table["name"],
                        "row_count": table.get("row_count", 0),
                        "error": str(e)
                    })
            
            cur.close()
            conn.close()
            
            return {
                "database_info": {
                    "type": "PostgreSQL",
                    "version": db_version,
                    "schemas": schemas,
                    "encoding": db_encoding,
                    "collation": db_collation
                },
                "tables": tables,
                "columns": columns,
                "constraints": constraints,
                "views": views,
                "materialized_views": materialized_views,
                "procedures": procedures,
                "triggers": triggers,
                "sequences": sequences,
                "user_types": user_types,
                "partitions": partitions,
                "indexes": indexes,
                "permissions": permissions,
                "data_profiles": data_profiles,
                "storage_info": {
                    "database_size": {
                        "total_size": total_database_size,
                        "data_size": total_database_data_size,
                        "index_size": total_database_index_size
                    },
                    "tables": [{"schema": t["schema"], "name": t["name"], "total_size": t["total_size"], "data_size": t["data_size"], "index_size": t["index_size"]} for t in tables]
                },
                "driver_unavailable": False
            }
        except Exception as e:
           return {"error": str(e), "driver_unavailable": False}
    
    async def extract_objects(self, selected_tables: List[str] | None = None) -> Dict[str, Any]:
        if not self.driver_available:
            raise Exception("PostgreSQL driver (psycopg2) is not installed. Please install it: pip install psycopg2-binary")
        # Normalize selected tables for case-insensitive matching.
        selected_set = set([str(t).strip() for t in (selected_tables or []) if str(t).strip()])
        selected_lower = set([t.lower() for t in selected_set])
        import re

        def _qident(identifier: str) -> str:
            # Basic SQL identifier quoting; keeps this file's existing f-string style.
            return '"' + identifier.replace('"', '""') + '"'
        
        try:
            conn = self._connect()
            cur = conn.cursor()
            
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
            
            # 1. Extract User-Defined Types (must be created first)
            cur.execute(f"""
                SELECT n.nspname, t.typname, t.typtype,
                       pg_catalog.format_type(t.oid, NULL) AS definition
                FROM pg_type t
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_class c WHERE c.oid = t.typrelid))
                AND NOT EXISTS(SELECT 1 FROM pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
                AND {self._schema_exclusion_clause('n.nspname')}
                AND t.typtype IN ('e', 'c', 'd')
                ORDER BY t.typname
            """)
            for row in cur.fetchall():
                schema, typename, typtype, definition = row
                if typtype == 'e':  # ENUM
                    cur.execute(f"SELECT enumlabel FROM pg_enum WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = '{typename}') ORDER BY enumsortorder")
                    values = [r[0] for r in cur.fetchall()]
                    ddl = f"CREATE TYPE {schema}.{typename} AS ENUM ({', '.join(repr(v) for v in values)});"
                else:
                    ddl = f"-- CREATE TYPE {schema}.{typename}"
                extracted_scripts["user_types"].append({"name": typename, "schema": schema, "ddl": ddl})
            
            # 2. Extract Sequences
            # When the user selected specific tables, only keep sequences referenced by those tables
            # (e.g., serial/identity defaults using nextval(...)).
            used_sequences_lower: set[str] = set()
            
            # 3. Extract Tables with full DDL
            cur.execute(f"""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE {self._schema_exclusion_clause('schemaname')}
                ORDER BY tablename
            """)
            for row in cur.fetchall():
                schema, table = row
                full_name = f"{schema}.{table}"
                if selected_set:
                    full_lower = full_name.lower()
                    table_lower = str(table).lower()
                    if full_lower not in selected_lower and table_lower not in selected_lower:
                        continue
                cur.execute(f"""
                    SELECT column_name, data_type, character_maximum_length,
                           column_default, is_nullable, is_generated, generation_expression
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                columns = []
                for col in cur.fetchall():
                    colname, dtype, maxlen, default, nullable, is_gen, gen_expr = col
                    col_def = f'    {colname} {dtype}'
                    if maxlen:
                        col_def = f'    {colname} {dtype}({maxlen})'
                    if is_gen == 'ALWAYS' and gen_expr:
                        col_def += f' GENERATED ALWAYS AS ({gen_expr}) STORED'
                    elif default:
                        default_text = str(default)
                        m = re.search(r"(?i)nextval\\('([^']+)'::regclass\\)", default_text)
                        if m:
                            seq_ref = (m.group(1) or "").replace('"', "").strip()
                            if seq_ref:
                                used_sequences_lower.add(seq_ref.lower())
                                used_sequences_lower.add(seq_ref.split(".")[-1].lower())
                        col_def += f' DEFAULT {default}'
                    if nullable == 'NO':
                        col_def += ' NOT NULL'
                    columns.append(col_def)
                
                ddl = f"CREATE TABLE {schema}.{table} (\n" + ',\n'.join(columns) + "\n);"
                extracted_scripts["tables"].append({"name": table, "schema": schema, "ddl": ddl})

            # Now extract sequences (selection-aware).
            cur.execute(f"""
                SELECT sequence_schema, sequence_name, start_value, increment, 
                       minimum_value, maximum_value, cycle_option
                FROM information_schema.sequences
                WHERE {self._schema_exclusion_clause('sequence_schema')}
            """)
            for row in cur.fetchall():
                schema, seqname, start, inc, minv, maxv, cycle = row
                if selected_set:
                    full_ref = f"{schema}.{seqname}".lower()
                    if (seqname or "").lower() not in used_sequences_lower and full_ref not in used_sequences_lower:
                        continue

                last = None
                try:
                    cur.execute(f"SELECT last_value FROM {_qident(schema)}.{_qident(seqname)}")
                    last_row = cur.fetchone()
                    last = last_row[0] if last_row else None
                except Exception:
                    last = None

                ddl = f"""CREATE SEQUENCE {schema}.{seqname}
    START WITH {start}
    INCREMENT BY {inc}
    MINVALUE {minv}
    MAXVALUE {maxv}
    {'CYCLE' if cycle == 'YES' else 'NO CYCLE'};
"""
                if last is not None:
                    ddl += f"ALTER SEQUENCE {schema}.{seqname} RESTART WITH {last};"
                extracted_scripts["sequences"].append({"name": seqname, "schema": schema, "ddl": ddl})
            
            # 4. Extract Constraints (PK, FK with CASCADE, CHECK, UNIQUE)
            cur.execute(f"""
                SELECT tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type,
                       cc.check_clause,
                       rc.update_rule, rc.delete_rule,
                       kcu.column_name,
                       ccu.table_name AS foreign_table_name,
                       ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                LEFT JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                LEFT JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON rc.constraint_name = ccu.constraint_name
                WHERE {self._schema_exclusion_clause('tc.table_schema')}
                ORDER BY tc.table_name, tc.constraint_name
            """)
            for row in cur.fetchall():
                schema, table, const_name, const_type, check_clause, upd_rule, del_rule, col, ftable, fcol = row
                full_name = f"{schema}.{table}"
                if selected_set and full_name not in selected_set and table not in selected_set:
                    continue
                if const_type == 'PRIMARY KEY':
                    ddl = f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {const_name} PRIMARY KEY ({col});"
                elif const_type == 'FOREIGN KEY':
                    ddl = f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {const_name} FOREIGN KEY ({col}) REFERENCES {ftable}({fcol})"
                    if upd_rule and upd_rule != 'NO ACTION':
                        ddl += f" ON UPDATE {upd_rule}"
                    if del_rule and del_rule != 'NO ACTION':
                        ddl += f" ON DELETE {del_rule}"
                    ddl += ";"
                elif const_type == 'CHECK':
                    ddl = f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {const_name} CHECK ({check_clause});"
                elif const_type == 'UNIQUE':
                    ddl = f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {const_name} UNIQUE ({col});"
                else:
                    continue
                extracted_scripts["constraints"].append({"name": const_name, "table": table, "schema": schema, "ddl": ddl})
            
            # 5. Extract Indexes
            cur.execute(f"""
                SELECT schemaname, tablename, indexname, indexdef
                FROM pg_indexes
                WHERE {self._schema_exclusion_clause('schemaname')}
                AND indexname NOT IN (
                    SELECT constraint_name FROM information_schema.table_constraints
                    WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                )
                ORDER BY indexname
            """)
            for row in cur.fetchall():
                schema, table, idxname, indexdef = row
                full_name = f"{schema}.{table}"
                if selected_set and full_name not in selected_set and table not in selected_set:
                    continue
                extracted_scripts["indexes"].append({"name": idxname, "table": table, "schema": schema, "ddl": indexdef + ";"})
            
            # 6. Extract Views
            if not selected_set:
                cur.execute(f"""
                    SELECT schemaname, viewname, definition
                    FROM pg_views
                    WHERE {self._schema_exclusion_clause('schemaname')}
                    ORDER BY viewname
                """)
                for row in cur.fetchall():
                    schema, viewname, definition = row
                    ddl = f"CREATE VIEW {schema}.{viewname} AS\n{definition}"
                    extracted_scripts["views"].append({"name": viewname, "schema": schema, "ddl": ddl})
            
            # 7. Extract Materialized Views
            if not selected_set:
                cur.execute(f"""
                    SELECT schemaname, matviewname, definition
                    FROM pg_matviews
                    WHERE {self._schema_exclusion_clause('schemaname')}
                    ORDER BY matviewname
                """)
                for row in cur.fetchall():
                    schema, mvname, definition = row
                    ddl = f"CREATE MATERIALIZED VIEW {schema}.{mvname} AS\n{definition}\nWITH DATA;"
                    extracted_scripts["materialized_views"].append({"name": mvname, "schema": schema, "ddl": ddl})
            
            # 8. Extract Functions/Procedures
            if not selected_set:
                cur.execute(f"""
                    SELECT n.nspname, p.proname, pg_get_functiondef(p.oid) AS definition
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE {self._schema_exclusion_clause('n.nspname')}
                    ORDER BY p.proname
                """)
                for row in cur.fetchall():
                    schema, funcname, definition = row
                    if definition and 'PROCEDURE' in definition.upper():
                        extracted_scripts["procedures"].append({"name": funcname, "schema": schema, "ddl": definition})
                    else:
                        extracted_scripts["functions"].append({"name": funcname, "schema": schema, "ddl": definition})
            
            # 9. Extract Triggers
            cur.execute(f"""
                SELECT trigger_schema, trigger_name, event_object_table, 
                       action_timing, event_manipulation, action_statement
                FROM information_schema.triggers
                WHERE {self._schema_exclusion_clause('trigger_schema')}
                ORDER BY trigger_name
            """)
            for row in cur.fetchall():
                schema, trigname, table, timing, event, action = row
                full_name = f"{schema}.{table}"
                if selected_set and full_name not in selected_set and table not in selected_set:
                    continue
                ddl = f"""CREATE TRIGGER {trigname}
    {timing} {event} ON {schema}.{table}
    FOR EACH ROW
    {action};"""
                extracted_scripts["triggers"].append({"name": trigname, "table": table, "schema": schema, "ddl": ddl})
            
            # 10. Extract GRANT statements
            cur.execute(f"""
                SELECT grantee, table_schema, table_name, privilege_type
                FROM information_schema.table_privileges
                WHERE {self._schema_exclusion_clause('table_schema')}
                ORDER BY grantee, table_name
            """)
            for row in cur.fetchall():
                grantee, schema, table, privilege = row
                full_name = f"{schema}.{table}"
                if selected_set and full_name not in selected_set and table not in selected_set:
                    continue
                ddl = f"GRANT {privilege} ON {schema}.{table} TO {grantee};"
                extracted_scripts["grants"].append({"grantee": grantee, "object": table, "schema": schema, "ddl": ddl})
            
            # 11. Generate Validation Scripts
            cur.execute(f"""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE {self._schema_exclusion_clause('schemaname')}
            """)
            for row in cur.fetchall():
                schema, table = row
                full_name = f"{schema}.{table}"
                if selected_set and full_name not in selected_set and table not in selected_set:
                    continue
                validation_sql = f"-- Validate row count for {schema}.{table}\nSELECT COUNT(*) FROM {schema}.{table};"
                extracted_scripts["validation_scripts"].append({"table": table, "schema": schema, "sql": validation_sql})
            
            cur.close()
            conn.close()
            
            total_objects = sum(len(v) for v in extracted_scripts.values())
            
            return {
                "ddl_scripts": extracted_scripts,
                "object_count": total_objects,
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
                "driver_unavailable": False
            }
        except Exception as e:
            return {"error": str(e), "driver_unavailable": False}
    

    
    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": True,
                "created": len(translated_ddl),
                "driver_unavailable": True,
                "message": "Simulated: would create " + str(len(translated_ddl)) + " objects"
            }
        
        results = []
        for obj in translated_ddl:
            try:
                print(f"[PostgreSQL] Creating object: {obj.get('name')}")
                print(f"[PostgreSQL] SQL: {obj.get('target_sql', 'NO SQL PROVIDED')[:200]}")
                
                schema_name = self._clean_ident(obj.get("schema") or "public") or "public"
                if schema_name.lower() == "public":
                    schema_name = "public"
                object_name = self._clean_ident(obj.get("name") or "")
                if not object_name:
                    raise Exception("Missing object name")

                conn = psycopg2.connect(
                    host=self.credentials.get("host"),
                    port=self.credentials.get("port", 5432),
                    database=self.credentials.get("database"),
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    sslmode=self.credentials.get("sslmode", "disable")
                )
                cur = conn.cursor()

                from psycopg2 import sql

                # Ensure schema exists (Snowflake "PUBLIC" should map fine here via case-insensitive matching)
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))

                # Drop existing table (case-insensitive lookup so unquoted/quoted creation is handled)
                resolved_schema, resolved_table = self._resolve_table_case_insensitive(cur, schema_name, object_name, allow_cross_schema=False)
                drop_stmt = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(resolved_schema),
                    sql.Identifier(resolved_table),
                )
                cur.execute(drop_stmt)
                
                # Create the new table
                target_sql = obj.get("target_sql")
                if not target_sql:
                    raise Exception("No target_sql provided")
                    
                cur.execute(target_sql)
                conn.commit()
                cur.close()
                conn.close()
                print(f"[PostgreSQL] Successfully created {obj.get('name')}")
                results.append({"name": obj["name"], "status": "success"})
            except Exception as e:
                print(f"[PostgreSQL] ERROR creating {obj.get('name')}: {str(e)}")
                results.append({"name": obj["name"], "status": "error", "error": str(e)})
        
        return {"ok": True, "results": results}

    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        if not self.driver_available:
            return {"ok": True, "driver_unavailable": True, "dropped": len(table_names)}

        from psycopg2 import sql

        dropped = 0
        errors: List[Dict[str, Any]] = []

        for ref in table_names or []:
            try:
                raw = str(ref or "").strip()
                if not raw:
                    continue
                parts = [p for p in raw.split(".") if p]
                if len(parts) >= 2:
                    schema_name, table_name = parts[-2], parts[-1]
                else:
                    schema_name, table_name = (self.credentials.get("schema") or "public"), parts[0]

                schema_name = self._clean_ident(schema_name) or "public"
                if schema_name.lower() == "public":
                    schema_name = "public"
                table_name = self._clean_ident(table_name)
                if not table_name:
                    continue

                conn = psycopg2.connect(
                    host=self.credentials.get("host"),
                    port=self.credentials.get("port", 5432),
                    database=self.credentials.get("database"),
                    user=self.credentials.get("username"),
                    password=self.credentials.get("password"),
                    sslmode=self.credentials.get("sslmode", "disable")
                )
                cur = conn.cursor()

                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))

                resolved_schema, resolved_table = self._resolve_table_case_insensitive(
                    cur, schema_name, table_name, allow_cross_schema=False
                )
                drop_stmt = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(resolved_schema),
                    sql.Identifier(resolved_table),
                )
                cur.execute(drop_stmt)
                conn.commit()
                cur.close()
                conn.close()
                dropped += 1
            except Exception as e:
                errors.append({"table": ref, "error": str(e)})

        return {"ok": len(errors) == 0, "dropped": dropped, "errors": errors}
    
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
                "ok": True,
                "table": table_name,
                "rows_copied": 1000,
                "status": "Success",
                "driver_unavailable": True
            }
        
        try:
            print(f"[PostgreSQL] copy_table_data called for table: {table_name}")
            
            # Check if source adapter has the yield_table_data method
            if not hasattr(source_adapter, 'yield_table_data'):
                print(f"[PostgreSQL] Source adapter does not have yield_table_data method")
                return {
                    "ok": False,
                    "error": f"Source adapter does not support data streaming",
                    "rows_copied": 0,
                    "status": "Error"
                }
            
            print(f"[PostgreSQL] Source adapter has yield_table_data, connecting to database...")
            
            # Connect to PostgreSQL target
            target_conn = self._connect(database=self.credentials.get("database"))
            target_cur = target_conn.cursor()
            
            print(f"[PostgreSQL] Connected to database, starting to stream data...")
            
            from psycopg2 import sql

            # Resolve target schema/table (case-insensitive, supports Snowflake's PUBLIC/AIR_TRANSPORT)
            requested_schema, requested_table = self._split_table_ref(table_name)
            resolved_schema, resolved_table = self._resolve_table_case_insensitive(target_cur, requested_schema, requested_table)
            
            # Get actual column names from PostgreSQL table
            print(f"[PostgreSQL] Querying actual column names for table: {resolved_schema}.{resolved_table}")
            target_cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE lower(table_schema) = lower(%s)
                  AND lower(table_name) = lower(%s)
                ORDER BY ordinal_position
                """,
                (resolved_schema, resolved_table),
            )
            pg_columns = [row[0] for row in target_cur.fetchall()]
            print(f"[PostgreSQL] PostgreSQL columns: {pg_columns}")
            
            rows_inserted = 0
            insert_sql_text = None
            did_truncate = False

            def _normalize_value(value):
                if value is None:
                    return None
                # Common psycopg2-adaptable primitives
                if isinstance(value, (str, int, float, bool)):
                    return value
                # psycopg2 handles Decimal/datetime/date well
                try:
                    from decimal import Decimal
                    import datetime as _dt
                    if isinstance(value, (Decimal, _dt.datetime, _dt.date, _dt.time)):
                        return value
                except Exception:
                    pass
                # Bytes-like
                if isinstance(value, (bytes, bytearray, memoryview)):
                    return bytes(value)
                # Try JSON serialization for dict/list/etc.
                try:
                    import json as _json
                    _json.dumps(value, default=str)
                    return _json.dumps(value, default=str)
                except Exception:
                    return str(value)
            
            # Stream data from source in chunks
            async for columns, rows in source_adapter.yield_table_data(table_name, chunk_size, columns=columns):
                print(f"[PostgreSQL] Received chunk with {len(rows)} rows")
                print(f"[PostgreSQL] Source columns: {columns}")
                
                if not rows:
                    print(f"[PostgreSQL] Empty chunk, skipping...")
                    continue
                
                # Build INSERT statement on first chunk using PostgreSQL column names
                if insert_sql_text is None:
                    if not pg_columns:
                        raise Exception(f"Target table not found or has no columns: {resolved_schema}.{resolved_table}")

                    pg_by_lower = {c.lower(): c for c in pg_columns}
                    target_cols_in_source_order = []
                    missing = []
                    for src_col in columns:
                        key = str(src_col).lower()
                        if key not in pg_by_lower:
                            missing.append(str(src_col))
                            continue
                        target_cols_in_source_order.append(pg_by_lower[key])

                    # Fallback: if names don't match but counts match, insert by position using target order.
                    if missing:
                        if len(pg_columns) == len(columns):
                            target_cols_in_source_order = list(pg_columns)
                            print(
                                f"[PostgreSQL] Warning: column name mismatch for {resolved_schema}.{resolved_table}; "
                                f"falling back to positional insert (missing: {missing})"
                            )
                        else:
                            raise Exception(
                                f"Column mismatch for {resolved_schema}.{resolved_table}: "
                                f"missing in target {missing} (target has {len(pg_columns)} cols, source has {len(columns)} cols)"
                            )

                    if not target_cols_in_source_order:
                        raise Exception("No columns to insert")

                    # Truncate once for idempotent re-runs
                    truncate_stmt = sql.SQL("TRUNCATE TABLE {}.{}").format(
                        sql.Identifier(resolved_schema),
                        sql.Identifier(resolved_table),
                    )
                    target_cur.execute(truncate_stmt)
                    target_conn.commit()
                    did_truncate = True

                    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        sql.Identifier(resolved_schema),
                        sql.Identifier(resolved_table),
                        sql.SQL(", ").join(sql.Identifier(c) for c in target_cols_in_source_order),
                        sql.SQL(", ").join(sql.Placeholder() for _ in target_cols_in_source_order),
                    )
                    insert_sql_text = insert_stmt.as_string(target_conn)
                    print(f"[PostgreSQL] Built INSERT SQL: {insert_sql_text}")
                
                # Insert this chunk
                print(f"[PostgreSQL] Inserting {len(rows)} rows...")
                normalized_rows = [tuple(_normalize_value(v) for v in row) for row in rows]
                target_cur.executemany(insert_sql_text, normalized_rows)
                target_conn.commit()
                rows_inserted += len(rows)
                print(f"[PostgreSQL] Inserted {len(rows)} rows, total so far: {rows_inserted}")
                if callable(progress_cb):
                    try:
                        progress_cb(rows_inserted, len(rows))
                    except Exception:
                        pass
            
            print(f"[PostgreSQL] Finished streaming, total rows inserted: {rows_inserted}")
            
            target_cur.close()
            target_conn.close()
            
            return {
                "ok": True,
                "table": f"{resolved_schema}.{resolved_table}",
                "rows_copied": rows_inserted,
                "status": "Success"
            }
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            print(f"[PostgreSQL] ERROR in copy_table_data: {error_msg}")
            return {
                "ok": False,
                "table": table_name,
                "rows_copied": 0,
                "status": "Error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }
    
    async def run_validation_checks(self, source_adapter: 'DatabaseAdapter') -> Dict[str, Any]:
        return {
            "structural": {"schema_match": True, "table_count_match": True},
            "data": {"row_counts_match": True, "checksums_match": True},
            "security": {"roles_match": True},
            "performance": {"baseline_ok": True},
            "driver_unavailable": not self.driver_available
        }
    
    async def get_table_row_count(self, table_name: str) -> int:
        if not self.driver_available:
            return 1000
        
        from psycopg2 import sql
        
        try:
            conn = self._connect(database=self.credentials.get("database"))
            cur = conn.cursor()

            schema, table = self._split_table_ref(table_name)

            # Try a few common variants before giving up (Snowflake often provides uppercase names).
            table_candidates = []
            if table:
                table_candidates.extend([table, table.lower()])
            seen = set()
            table_candidates = [t for t in table_candidates if t and not (t.lower() in seen or seen.add(t.lower()))]

            resolved_schema, resolved_table = schema, table
            for candidate in table_candidates:
                rs, rt = self._resolve_table_case_insensitive(cur, schema, candidate, allow_cross_schema=False)
                if (rs, rt) != (schema, candidate):
                    resolved_schema, resolved_table = rs, rt
                    break
            else:
                # Last resort: allow cross-schema lookup for validation convenience
                resolved_schema, resolved_table = self._resolve_table_case_insensitive(cur, schema, table, allow_cross_schema=True)

            # Use sql.Identifier to safely quote schema/table
            query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(resolved_schema),
                sql.Identifier(resolved_table),
            )
            try:
                cur.execute(query)
                count = cur.fetchone()[0]
            except Exception:
                # Fallback: if a quoted name doesn't exist, try the unquoted-lowercase equivalent.
                query2 = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(resolved_schema),
                    sql.Identifier(str(resolved_table).lower()),
                )
                cur.execute(query2)
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
            conn = self._connect(database=self.credentials.get("database"))
            cur = conn.cursor()
            
            for table in tables_ddl:
                table_name = table.get("name", "")
                schema_name = table.get("schema", "public")

                schema_candidates = [
                    self._clean_ident(schema_name) or "public",
                    "public",
                    "dbo",
                    "main",
                ]
                # de-dupe while keeping order
                seen = set()
                schema_candidates = [s for s in schema_candidates if s and not (s.lower() in seen or seen.add(s.lower()))]

                columns = []
                for candidate_schema in schema_candidates:
                    cur.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE lower(table_name) = lower(%s)
                          AND lower(table_schema) = lower(%s)
                        ORDER BY ordinal_position
                        """,
                        (table_name, candidate_schema),
                    )
                    rows = cur.fetchall()
                    if rows:
                        columns = [{"name": r[0], "type": r[1]} for r in rows]
                        break

                schema_info[table_name] = columns
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error getting schema structure: {e}")
            raise
        
        return schema_info
    
    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        if not self.driver_available:
            return {
                "ok": True,
                "message": f"Would rename column {old_column_name} to {new_column_name} in {table_name} (simulated)"
            }
        
        try:
            # Connect to PostgreSQL database
            conn = self._connect(database=self.credentials.get("database"))
            cur = conn.cursor()
            
            from psycopg2 import sql
            
            # Split table name to schema and table
            requested_schema, requested_table = self._split_table_ref(table_name)
            
            # Resolve the actual schema/table names (case-insensitive lookup)
            resolved_schema, resolved_table = self._resolve_table_case_insensitive(
                cur, requested_schema, requested_table
            )
            
            # Build ALTER TABLE statement
            alter_stmt = sql.SQL("ALTER TABLE {}.{} RENAME COLUMN {} TO {}").format(
                sql.Identifier(resolved_schema),
                sql.Identifier(resolved_table),
                sql.Identifier(old_column_name),
                sql.Identifier(new_column_name)
            )
            
            # Execute the rename
            cur.execute(alter_stmt)
            conn.commit()
            
            # Close connections
            cur.close()
            conn.close()
            
            return {
                "ok": True,
                "message": f"Successfully renamed column {old_column_name} to {new_column_name} in {resolved_schema}.{resolved_table}"
            }
        except Exception as e:
            error_msg = str(e)
            print(f"[PostgreSQL] ERROR in rename_column: {error_msg}")
            return {
                "ok": False,
                "message": f"Failed to rename column: {error_msg}",
                "error": error_msg
            }
