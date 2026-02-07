import sqlite3
import json
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import aiosqlite

# asyncpg is optional; only needed when DATABASE_URL is set
try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

DATABASE_URL = os.getenv("DATABASE_URL")
# Only enable Postgres if both DATABASE_URL is set and asyncpg is installed
USE_POSTGRES = bool(DATABASE_URL and asyncpg is not None)
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "strata.db")

_pg_pool = None

async def get_pg_pool():
    """Get or create PostgreSQL connection pool"""
    global _pg_pool
    if asyncpg is None:
        raise RuntimeError(
            "PostgreSQL support requires asyncpg. Install backend requirements or unset DATABASE_URL to use SQLite."
        )
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pg_pool

async def init_db():
    """Initialize database tables - PostgreSQL or SQLite based on environment"""
    if USE_POSTGRES:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS connections (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    db_type TEXT NOT NULL,
                    enc_credentials BYTEA NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id SERIAL PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    structure_started_at TEXT,
                    data_completed_at TEXT,
                    completed_at TEXT,
                    duration_ms INTEGER,
                    structure_data_duration_ms INTEGER,
                    source_id INTEGER REFERENCES connections(id),
                    target_id INTEGER REFERENCES connections(id),
                    status TEXT NOT NULL,
                    migrated_rows INTEGER DEFAULT 0,
                    failed_rows INTEGER DEFAULT 0
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES runs(id),
                    kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS validation_reports (
                    id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES runs(id),
                    json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            # Create active_session table with selected_tables column
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS active_session (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    source_id INTEGER REFERENCES connections(id),
                    target_id INTEGER REFERENCES connections(id),
                    run_id INTEGER REFERENCES runs(id),
                    selected_tables TEXT,
                    selected_columns TEXT,
                    column_renames TEXT
                )
            """)
            try:
                await conn.execute("ALTER TABLE active_session ADD COLUMN IF NOT EXISTS selected_columns TEXT")
            except Exception:
                pass
            try:
                await conn.execute("ALTER TABLE active_session ADD COLUMN IF NOT EXISTS column_renames TEXT")
            except Exception:
                pass
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    db_type TEXT NOT NULL,
                    enc_credentials BLOB NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    structure_started_at TEXT,
                    data_completed_at TEXT,
                    completed_at TEXT,
                    duration_ms INTEGER,
                    structure_data_duration_ms INTEGER,
                    source_id INTEGER,
                    target_id INTEGER,
                    status TEXT NOT NULL,
                    migrated_rows INTEGER DEFAULT 0,
                    failed_rows INTEGER DEFAULT 0,
                    FOREIGN KEY (source_id) REFERENCES connections(id),
                    FOREIGN KEY (target_id) REFERENCES connections(id)
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS validation_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
            """)
            
            # Create active_session table with selected_tables column
            await db.execute("""
                CREATE TABLE IF NOT EXISTS active_session (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    source_id INTEGER,
                    target_id INTEGER,
                    run_id INTEGER,
                    selected_tables TEXT,
                    selected_columns TEXT,
                    column_renames TEXT,
                    FOREIGN KEY (source_id) REFERENCES connections(id),
                    FOREIGN KEY (target_id) REFERENCES connections(id),
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
            """)
            
            # Backfill missing columns for runs if schema was created earlier
            async with db.execute("PRAGMA table_info(runs)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
            desired = {
                "started_at": "TEXT",
                "structure_started_at": "TEXT",
                "data_completed_at": "TEXT",
                "completed_at": "TEXT",
                "duration_ms": "INTEGER",
                "structure_data_duration_ms": "INTEGER",
                "migrated_rows": "INTEGER DEFAULT 0",
                "failed_rows": "INTEGER DEFAULT 0"
            }
            for col, col_def in desired.items():
                if col not in columns:
                    try:
                        await db.execute(f"ALTER TABLE runs ADD COLUMN {col} {col_def}")
                    except Exception:
                        pass

            async with db.execute("PRAGMA table_info(active_session)") as cursor:
                session_columns = [row[1] for row in await cursor.fetchall()]
            if "selected_columns" not in session_columns:
                try:
                    await db.execute("ALTER TABLE active_session ADD COLUMN selected_columns TEXT")
                except Exception:
                    pass
            
            if "column_renames" not in session_columns:
                try:
                    await db.execute("ALTER TABLE active_session ADD COLUMN column_renames TEXT")
                except Exception:
                    pass
            
            await db.commit()

class ConnectionModel:
    @staticmethod
    async def create(name: str, db_type: str, enc_credentials: bytes):
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO connections (name, db_type, enc_credentials, created_at) VALUES ($1, $2, $3, $4) RETURNING id",
                    name, db_type, enc_credentials, datetime.utcnow().isoformat()
                )
                return row['id']
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                cursor = await db.execute(
                    "INSERT INTO connections (name, db_type, enc_credentials, created_at) VALUES (?, ?, ?, ?)",
                    (name, db_type, enc_credentials, datetime.utcnow().isoformat())
                )
                await db.commit()
                return cursor.lastrowid
    
    @staticmethod
    async def get_all():
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT id, name, db_type, enc_credentials, created_at FROM connections")
                return [dict(row) for row in rows]
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT id, name, db_type, enc_credentials, created_at FROM connections") as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
    
    @staticmethod
    async def get_by_id(conn_id: int):
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM connections WHERE id = $1", conn_id)
                return dict(row) if row else None
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM connections WHERE id = ?", (conn_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

class SessionModel:
    @staticmethod
    async def set_session(source_id: int, target_id: int, run_id: int, selected_tables: list = None, selected_columns: dict = None, column_renames: dict = None):
        # Serialize selected_tables to JSON string for storage
        selected_tables_json = json.dumps(selected_tables) if selected_tables else None
        selected_columns_json = json.dumps(selected_columns) if selected_columns is not None else None
        # Preserve previously saved renames when not explicitly provided.
        column_renames_json = json.dumps(column_renames) if column_renames is not None else None
        
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                existing = await conn.fetchrow("SELECT selected_columns, column_renames FROM active_session WHERE id = 1")
                if selected_columns_json is None and existing and existing.get("selected_columns"):
                    selected_columns_json = existing["selected_columns"]
                if column_renames_json is None and existing and existing.get("column_renames"):
                    column_renames_json = existing["column_renames"]
                await conn.execute("DELETE FROM active_session")
                await conn.execute(
                    "INSERT INTO active_session (id, source_id, target_id, run_id, selected_tables, selected_columns, column_renames) VALUES (1, $1, $2, $3, $4, $5, $6)",
                    source_id, target_id, run_id, selected_tables_json, selected_columns_json, column_renames_json
                )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                existing = None
                async with db.execute("SELECT selected_columns, column_renames FROM active_session WHERE id = 1") as cursor:
                    existing = await cursor.fetchone()
                if selected_columns_json is None and existing and existing[0]:
                    selected_columns_json = existing[0]
                if column_renames_json is None and existing and existing[1]:
                    column_renames_json = existing[1]
                await db.execute("DELETE FROM active_session")
                await db.execute(
                    "INSERT INTO active_session (id, source_id, target_id, run_id, selected_tables, selected_columns, column_renames) VALUES (1, ?, ?, ?, ?, ?, ?)",
                    (source_id, target_id, run_id, selected_tables_json, selected_columns_json, column_renames_json)
                )
                await db.commit()
    
    @staticmethod
    async def get_session():
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM active_session WHERE id = 1")
                result = dict(row) if row else None
                # Deserialize selected_tables from JSON string
                if result and result.get('selected_tables'):
                    try:
                        result['selected_tables'] = json.loads(result['selected_tables'])
                    except:
                        result['selected_tables'] = []
                if result and result.get('selected_columns'):
                    try:
                        result['selected_columns'] = json.loads(result['selected_columns'])
                    except:
                        result['selected_columns'] = {}
                if result and result.get('column_renames'):
                    try:
                        result['column_renames'] = json.loads(result['column_renames'])
                    except:
                        result['column_renames'] = {}
                return result
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM active_session WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    result = dict(row) if row else None
                    # Deserialize selected_tables from JSON string
                    if result and result.get('selected_tables'):
                        try:
                            result['selected_tables'] = json.loads(result['selected_tables'])
                        except:
                            result['selected_tables'] = []
                    if result and result.get('selected_columns'):
                        try:
                            result['selected_columns'] = json.loads(result['selected_columns'])
                        except:
                            result['selected_columns'] = {}
                    if result and result.get('column_renames'):
                        try:
                            result['column_renames'] = json.loads(result['column_renames'])
                        except:
                            result['column_renames'] = {}
                    return result
    
    @staticmethod
    async def clear_session():
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM active_session")
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute("DELETE FROM active_session")
                await db.commit()

    @staticmethod
    async def clear_column_renames():
        """Explicitly clear column renames from the active session."""
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute("UPDATE active_session SET column_renames = NULL WHERE id = 1")
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute("UPDATE active_session SET column_renames = NULL WHERE id = 1")
                await db.commit()

    @staticmethod
    async def set_selected_tables(selected_tables: list):
        """Update only the selected_tables field in the active session"""
        selected_tables_json = json.dumps(selected_tables) if selected_tables else None
        
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                # First check if a session exists
                row = await conn.fetchrow("SELECT id FROM active_session WHERE id = 1")
                if row:
                    # Update existing session
                    await conn.execute(
                        "UPDATE active_session SET selected_tables = $1 WHERE id = 1",
                        selected_tables_json
                    )
                else:
                    # Create new session with only selected_tables (other fields will be null)
                    await conn.execute(
                        "INSERT INTO active_session (id, selected_tables) VALUES (1, $1)",
                        selected_tables_json
                    )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                # First check if a session exists
                async with db.execute("SELECT id FROM active_session WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                
                if row:
                    # Update existing session
                    await db.execute(
                        "UPDATE active_session SET selected_tables = ? WHERE id = 1",
                        (selected_tables_json,)
                    )
                else:
                    # Create new session with only selected_tables (other fields will be null)
                    await db.execute(
                        "INSERT INTO active_session (id, selected_tables) VALUES (1, ?)",
                        (selected_tables_json,)
                    )
                await db.commit()

    @staticmethod
    async def set_selected_columns(selected_columns: dict):
        """Update only the selected_columns field in the active session"""
        selected_columns_json = json.dumps(selected_columns) if selected_columns else None

        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT id FROM active_session WHERE id = 1")
                if row:
                    await conn.execute(
                        "UPDATE active_session SET selected_columns = $1 WHERE id = 1",
                        selected_columns_json
                    )
                else:
                    await conn.execute(
                        "INSERT INTO active_session (id, selected_columns) VALUES (1, $1)",
                        selected_columns_json
                    )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                async with db.execute("SELECT id FROM active_session WHERE id = 1") as cursor:
                    row = await cursor.fetchone()

                if row:
                    await db.execute(
                        "UPDATE active_session SET selected_columns = ? WHERE id = 1",
                        (selected_columns_json,)
                    )
                else:
                    await db.execute(
                        "INSERT INTO active_session (id, selected_columns) VALUES (1, ?)",
                        (selected_columns_json,)
                    )
                await db.commit()

    @staticmethod
    async def set_column_renames(column_renames: dict):
        """Update only the column_renames field in the active session"""
        column_renames_json = json.dumps(column_renames) if column_renames else None

        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT id FROM active_session WHERE id = 1")
                if row:
                    await conn.execute(
                        "UPDATE active_session SET column_renames = $1 WHERE id = 1",
                        column_renames_json
                    )
                else:
                    await conn.execute(
                        "INSERT INTO active_session (id, column_renames) VALUES (1, $1)",
                        column_renames_json
                    )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                async with db.execute("SELECT id FROM active_session WHERE id = 1") as cursor:
                    row = await cursor.fetchone()

                if row:
                    await db.execute(
                        "UPDATE active_session SET column_renames = ? WHERE id = 1",
                        (column_renames_json,)
                    )
                else:
                    await db.execute(
                        "INSERT INTO active_session (id, column_renames) VALUES (1, ?)",
                        (column_renames_json,)
                    )
                await db.commit()

    @staticmethod
    async def get_column_renames():
        """Retrieve only the column_renames field from the active session"""
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT column_renames FROM active_session WHERE id = 1")
                if row and row['column_renames']:
                    try:
                        return json.loads(row['column_renames'])
                    except:
                        return {}
                return {}
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT column_renames FROM active_session WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    if row and row['column_renames']:
                        try:
                            return json.loads(row['column_renames'])
                        except:
                            return {}
                    return {}

class RunModel:
    @staticmethod
    async def create(source_id: Optional[int], target_id: Optional[int]):
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO runs (created_at, started_at, source_id, target_id, status, migrated_rows, failed_rows) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    source_id,
                    target_id,
                    "started",
                    0,
                    0
                )
                return row['id']
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                cursor = await db.execute(
                    "INSERT INTO runs (created_at, started_at, source_id, target_id, status, migrated_rows, failed_rows) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        datetime.now(timezone.utc).isoformat(),
                        source_id,
                        target_id,
                        "started",
                        0,
                        0
                    )
                )
                await db.commit()
                return cursor.lastrowid

    @staticmethod
    async def update_connections(run_id: int, source_id: Optional[int] = None, target_id: Optional[int] = None):
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE runs
                    SET source_id = COALESCE($1, source_id),
                        target_id = COALESCE($2, target_id)
                    WHERE id = $3
                    """,
                    source_id,
                    target_id,
                    run_id
                )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute(
                    """
                    UPDATE runs
                    SET source_id = COALESCE(?, source_id),
                        target_id = COALESCE(?, target_id)
                    WHERE id = ?
                    """,
                    (source_id, target_id, run_id)
                )
                await db.commit()
    
    @staticmethod
    async def get(run_id: int) -> Optional[Dict[str, Any]]:
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM runs WHERE id = $1", run_id)
                return dict(row) if row else None
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

    @staticmethod
    async def update_status(run_id: int, status: str, migrated_rows: Optional[int] = None, failed_rows: Optional[int] = None, mark_complete: bool = False, mark_structure_start: bool = False, mark_data_complete: bool = False):
        now = datetime.now(timezone.utc).isoformat()
        existing = await RunModel.get(run_id)
        duration_ms = None
        structure_data_duration_ms = None
        completed_at = None
        structure_started_at = None
        data_completed_at = None
        
        if mark_complete:
            completed_at = now
            if existing and existing.get("started_at"):
                try:
                    start_dt = datetime.fromisoformat(existing["started_at"])
                    end_dt = datetime.fromisoformat(now)
                    duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
                except Exception:
                    duration_ms = None
        
        if mark_structure_start:
            structure_started_at = now
        
        if mark_data_complete:
            data_completed_at = now
            # Calculate structure-to-data duration
            if existing and existing.get("structure_started_at"):
                try:
                    structure_start_dt = datetime.fromisoformat(existing["structure_started_at"])
                    data_end_dt = datetime.fromisoformat(now)
                    structure_data_duration_ms = int((data_end_dt - structure_start_dt).total_seconds() * 1000)
                except Exception:
                    # Try with the current time if structure start wasn't recorded
                    try:
                        structure_start_dt = datetime.fromisoformat(now)
                        data_end_dt = datetime.fromisoformat(now)
                        structure_data_duration_ms = int((data_end_dt - structure_start_dt).total_seconds() * 1000)
                    except Exception:
                        structure_data_duration_ms = None

        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE runs
                    SET status = COALESCE($1, status),
                        migrated_rows = COALESCE($2, migrated_rows),
                        failed_rows = COALESCE($3, failed_rows),
                        structure_started_at = CASE WHEN $4 THEN $5 ELSE structure_started_at END,
                        data_completed_at = CASE WHEN $6 THEN $7 ELSE data_completed_at END,
                        completed_at = CASE WHEN $8 THEN $9 ELSE completed_at END,
                        duration_ms = CASE WHEN $8 AND $10 IS NOT NULL THEN $10 ELSE duration_ms END,
                        structure_data_duration_ms = CASE WHEN $6 AND $11 IS NOT NULL THEN $11 ELSE structure_data_duration_ms END
                    WHERE id = $12
                    """,
                    status, migrated_rows, failed_rows, mark_structure_start, structure_started_at,
                    mark_data_complete, data_completed_at, mark_complete, completed_at,
                    duration_ms, structure_data_duration_ms, run_id
                )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute(
                    """
                    UPDATE runs
                    SET status = COALESCE(?, status),
                        migrated_rows = COALESCE(?, migrated_rows),
                        failed_rows = COALESCE(?, failed_rows),
                        structure_started_at = CASE WHEN ? THEN ? ELSE structure_started_at END,
                        data_completed_at = CASE WHEN ? THEN ? ELSE data_completed_at END,
                        completed_at = CASE WHEN ? THEN ? ELSE completed_at END,
                        duration_ms = CASE WHEN ? AND ? IS NOT NULL THEN ? ELSE duration_ms END,
                        structure_data_duration_ms = CASE WHEN ? AND ? IS NOT NULL THEN ? ELSE structure_data_duration_ms END
                    WHERE id = ?
                    """,
                    (status, migrated_rows, failed_rows, mark_structure_start, structure_started_at,
                     mark_data_complete, data_completed_at, mark_complete, completed_at,
                     mark_complete, duration_ms, duration_ms, mark_data_complete, structure_data_duration_ms, structure_data_duration_ms, run_id)
                )
                await db.commit()

    @staticmethod
    async def list_runs(limit: int = 50) -> List[Dict[str, Any]]:
        import json
        import os
        
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT r.*, 
                        src.name AS source_name, src.db_type AS source_type,
                        tgt.name AS target_name, tgt.db_type AS target_type
                    FROM runs r
                    LEFT JOIN connections src ON r.source_id = src.id
                    LEFT JOIN connections tgt ON r.target_id = tgt.id
                    ORDER BY COALESCE(r.completed_at, r.started_at, r.created_at) DESC
                    LIMIT $1
                    """,
                    limit
                )
                runs = [dict(row) for row in rows]
                
                # Add table count from extraction results
                for run in runs:
                    run_id = run.get('id')
                    if run_id:
                        try:
                            # Look for extraction results in artifacts
                            extraction_file = f"artifacts/extraction_{run_id}.json"
                            if os.path.exists(extraction_file):
                                with open(extraction_file, 'r') as f:
                                    extraction_data = json.load(f)
                                    ddl_scripts = extraction_data.get('ddl_scripts', {})
                                    tables = ddl_scripts.get('tables', [])
                                    run['table_count'] = len(tables)
                            else:
                                # Look for analysis results as fallback
                                analysis_file = f"artifacts/analysis_{run_id}.json"
                                if os.path.exists(analysis_file):
                                    with open(analysis_file, 'r') as f:
                                        analysis_data = json.load(f)
                                        tables = analysis_data.get('tables', [])
                                        run['table_count'] = len(tables)
                                else:
                                    run['table_count'] = 0
                        except Exception:
                            run['table_count'] = 0
                
                return runs
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    """
                    SELECT r.*, 
                        src.name AS source_name, src.db_type AS source_type,
                        tgt.name AS target_name, tgt.db_type AS target_type
                    FROM runs r
                    LEFT JOIN connections src ON r.source_id = src.id
                    LEFT JOIN connections tgt ON r.target_id = tgt.id
                    ORDER BY COALESCE(r.completed_at, r.started_at, r.created_at) DESC
                    LIMIT ?
                    """,
                    (limit,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    runs = [dict(row) for row in rows]
                    
                    # Add table count from extraction results
                    for run in runs:
                        run_id = run.get('id')
                        if run_id:
                            try:
                                # Look for extraction results in artifacts
                                extraction_file = f"artifacts/extraction_{run_id}.json"
                                if os.path.exists(extraction_file):
                                    with open(extraction_file, 'r') as f:
                                        extraction_data = json.load(f)
                                        ddl_scripts = extraction_data.get('ddl_scripts', {})
                                        tables = ddl_scripts.get('tables', [])
                                        run['table_count'] = len(tables)
                                else:
                                    # Look for analysis results as fallback
                                    analysis_file = f"artifacts/analysis_{run_id}.json"
                                    if os.path.exists(analysis_file):
                                        with open(analysis_file, 'r') as f:
                                            analysis_data = json.load(f)
                                            tables = analysis_data.get('tables', [])
                                            run['table_count'] = len(tables)
                                    else:
                                        run['table_count'] = 0
                            except Exception:
                                run['table_count'] = 0
                    
                    return runs

    @staticmethod
    async def delete(run_id: int) -> bool:
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                result = await conn.execute("DELETE FROM runs WHERE id = $1", run_id)
                # asyncpg returns strings like "DELETE 1"
                return result.split()[-1] != "0"
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                cursor = await db.execute("DELETE FROM runs WHERE id = ?", (run_id,))
                await db.commit()
                return cursor.rowcount > 0

    @staticmethod
    async def delete_all() -> int:
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                result = await conn.execute("DELETE FROM runs")
                return int(result.split()[-1])
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                cursor = await db.execute("DELETE FROM runs")
                await db.commit()
                return cursor.rowcount or 0
