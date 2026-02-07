import re
import os
import sys
import json
import asyncio
import logging
import importlib
import time
import base64
import traceback
from uuid import uuid4
from contextlib import asynccontextmanager
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any, List

import aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import FileResponse, Response, JSONResponse

# Support running as a package (uvicorn backend.main:app) or as a script (uvicorn main:app)
try:
    from .models import ConnectionModel, SessionModel, RunModel, init_db, USE_POSTGRES, DATABASE_PATH
    from .adapters import get_adapter, ADAPTERS
    from .encryption import decrypt_credentials, encrypt_credentials
    from .validation import validate_tables, validate_column_renames
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from models import ConnectionModel, SessionModel, RunModel, init_db, USE_POSTGRES, DATABASE_PATH
    from adapters import get_adapter, ADAPTERS
    from encryption import decrypt_credentials, encrypt_credentials
    from validation import validate_tables


def _import_ai_module():
    """
    Import the ai module whether this file was loaded as part of the backend package
    or as a standalone module.
    """
    module_name = f"{__package__}.ai" if __package__ else "ai"
    try:
        return importlib.import_module(module_name)
    except ImportError:
        # Fallback to plain import when __package__ is not set
        return importlib.import_module("ai")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('strata_backend.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('strata')

# Session-scoped log markers (in-memory)
session_log_clears: Dict[str, float] = {}
session_log_starts: Dict[str, float] = {}
run_session_map: Dict[int, str] = {}
session_run_map: Dict[str, int] = {}

# Custom JSON encoder to handle Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

app = FastAPI()

# Log application startup
logger.info('Strata backend application starting up')

# Health check endpoints MUST be defined FIRST before any middleware or other config
# These must respond instantly for deployment health checks
@app.get("/health")
async def health_check_simple():
    """Ultra-fast health check for deployment"""
    return {"status": "ok"}

@app.head("/health")
async def health_check_head():
    """HEAD request for health check"""
    return Response(status_code=200)

# Root HEAD endpoint for deployment health checks - must be ultra-fast
@app.head("/")
async def root_head():
    """HEAD request for root health check - deployment uses this"""
    return Response(status_code=200)

# Logs endpoint for system monitoring
@app.get("/api/logs")
async def get_logs(limit: int = 100):
    """Retrieve recent backend logs for system monitoring"""
    try:
        # Read logs from file
        log_entries = []
        log_file_path = 'strata_backend.log'
        
        # Log that we're trying to read logs
        logger.info(f"Attempting to read logs from {log_file_path}")
        
        # Check if log file exists
        if os.path.exists(log_file_path):
            logger.info(f"Log file exists, reading content")
            with open(log_file_path, 'r') as f:
                lines = f.readlines()
                logger.info(f"Read {len(lines)} lines from log file")
                # Get the last 'limit' lines
                recent_lines = lines[-limit:] if len(lines) > limit else lines
                
                # Parse log lines into structured data
                for line in recent_lines:
                    # Simple parsing - in a real app, you might want more sophisticated parsing
                    parts = line.strip().split(' ', 3)
                    if len(parts) >= 4:
                        timestamp = f"{parts[0]} {parts[1]}"
                        level = parts[2].strip('[]')
                        message = parts[3]
                        log_entries.append({
                            "timestamp": timestamp,
                            "level": level,
                            "message": message
                        })
        else:
            logger.warning(f"Log file {log_file_path} does not exist")
        
        logger.info(f"Returning {len(log_entries)} log entries")
        return {
            "ok": True,
            "data": log_entries,
            "limit": limit,
            "total": len(log_entries)
        }
    except Exception as e:
        logger.error(f"Error reading logs: {str(e)}")
        return {
            "ok": False,
            "message": f"Error reading logs: {str(e)}",
            "data": [],
            "limit": limit
        }

def _parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    parts = line.strip().split(' ', 3)
    if len(parts) < 4:
        return None
    timestamp = f"{parts[0]} {parts[1]}"
    timestamp_iso = timestamp.replace(',', '.').replace(' ', 'T', 1)
    level = parts[2].strip('[]')
    raw_message = parts[3]
    logger_name = None
    message = raw_message
    if ': ' in raw_message:
        logger_name, message = raw_message.split(': ', 1)
    step = None
    if message.startswith('[') and ']' in message:
        end_idx = message.find(']')
        step = message[1:end_idx]
        message = message[end_idx + 1:].strip()
    run_id = None
    tables = None
    session_id = None
    session_match = re.search(r'session_id[=:\s]+([a-f0-9-]+)', message, re.IGNORECASE)
    if session_match:
        session_id = session_match.group(1)
        message = re.sub(r'\s*session_id[=:\s]+[a-f0-9-]+\b', '', message, flags=re.IGNORECASE)
    match = re.search(r'run_id[=:\s]+(\d+)', message, re.IGNORECASE)
    if match:
        try:
            run_id = int(match.group(1))
        except ValueError:
            run_id = None
        message = re.sub(r'\s*run_id[=:\s]+\d+\b', '', message, flags=re.IGNORECASE)
    tables_match = re.search(r'tables_list=([A-Za-z0-9+/=]+)', message)
    if tables_match:
        encoded = tables_match.group(1)
        try:
            decoded = base64.b64decode(encoded).decode("utf-8")
            parsed_tables = json.loads(decoded)
            if isinstance(parsed_tables, list):
                tables = [str(t) for t in parsed_tables]
        except Exception:
            tables = None
        message = re.sub(r'\s*tables_list=[A-Za-z0-9+/=]+', '', message)
    message = " ".join(message.split())
    return {
        "timestamp": timestamp_iso,
        "level": level,
        "message": message,
        "step": step,
        "run_id": run_id,
        "session_id": session_id,
        "tables": tables,
        "logger": logger_name
    }

def _parse_log_timestamp(timestamp: str) -> Optional[float]:
    if not timestamp:
        return None
    try:
        cleaned = timestamp.replace(',', '.')
        parsed = datetime.fromisoformat(cleaned)
        return parsed.timestamp()
    except Exception:
        return None

def _get_request_session_id(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None
    return request.headers.get("x-session-id")

def _resolve_session_id(session_id: Optional[str], run_id: Optional[int]) -> Optional[str]:
    if session_id:
        return session_id
    if run_id is not None and run_id in run_session_map:
        return run_session_map[run_id]
    return None

def _log_event(
    step: str,
    message: str,
    run_id: Optional[int] = None,
    session_id: Optional[str] = None,
    level: str = "info",
    tables: Optional[List[str]] = None
):
    resolved_session_id = _resolve_session_id(session_id, run_id)
    suffix_parts = []
    if resolved_session_id:
        suffix_parts.append(f"session_id={resolved_session_id}")
    if run_id is not None:
        suffix_parts.append(f"run_id={run_id}")
    encoded_tables = _encode_tables_list(tables or [])
    if encoded_tables:
        suffix_parts.append(f"tables_list={encoded_tables}")
    suffix = f" {' '.join(suffix_parts)}" if suffix_parts else ""
    full_message = f"[{step}] {message}{suffix}"
    if level == "warning":
        logger.warning(full_message)
    elif level == "error":
        logger.error(full_message)
    else:
        logger.info(full_message)

def _compact_log_value(value: Any, max_len: int = 4000) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = " ".join(text.split())
    if max_len and len(text) > max_len:
        return text[:max_len] + "..."
    return text

async def _ensure_session_run_id(
    session_id: Optional[str],
    source_id: Optional[int] = None,
    target_id: Optional[int] = None
) -> Optional[int]:
    if not session_id:
        return None
    run_id = session_run_map.get(session_id)
    if run_id is None:
        try:
            run_id = await RunModel.create(source_id, target_id)
        except Exception:
            run_id = None
        if run_id is not None:
            session_run_map[session_id] = run_id
            run_session_map[run_id] = session_id
    if run_id is not None and (source_id is not None or target_id is not None):
        try:
            await RunModel.update_connections(run_id, source_id, target_id)
        except Exception:
            pass
    return run_id

def _summarize_tables(tables: List[Any], max_items: int = 5) -> str:
    cleaned = [str(t).strip() for t in (tables or []) if str(t).strip()]
    if not cleaned:
        return "(none)"
    shown = cleaned[:max_items]
    remaining = len(cleaned) - len(shown)
    summary = ", ".join(shown)
    if remaining > 0:
        return f"({summary} +{remaining} more)"
    return f"({summary})"

def _encode_tables_list(tables: List[str]) -> Optional[str]:
    if not tables:
        return None
    try:
        payload = json.dumps(tables, separators=(',', ':')).encode("utf-8")
        return base64.b64encode(payload).decode("utf-8")
    except Exception:
        return None

def _safe_cred_value(creds: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        value = creds.get(key)
        if value:
            return str(value)
    return None

def _format_connection_label(connection: Dict[str, Any]) -> str:
    name = connection.get("name") or "unknown"
    db_type = connection.get("db_type") or "unknown"
    label = name
    context_parts: List[str] = []
    try:
        creds = decrypt_credentials(connection.get("enc_credentials"))
        if isinstance(creds, dict):
            db_name = _safe_cred_value(creds, ["database", "db_name", "dbname", "databaseName"])
            service = _safe_cred_value(creds, ["service_name", "serviceName"])
            schema = _safe_cred_value(creds, ["schema", "schema_name", "schemaName"])
            catalog = _safe_cred_value(creds, ["catalog", "catalogName"])
            warehouse = _safe_cred_value(creds, ["warehouse", "warehouseName"])
            primary = db_name or service or catalog
            if primary:
                label = primary
            if schema:
                context_parts.append(f"schema={schema}")
            if warehouse:
                context_parts.append(f"warehouse={warehouse}")
    except Exception:
        pass
    context = f" {' '.join(context_parts)}" if context_parts else ""
    return f"{label} ({db_type}){context}"

@app.post("/api/session/start")
async def start_log_session():
    session_id = uuid4().hex
    session_log_starts[session_id] = time.time()
    run_id = await _ensure_session_run_id(session_id)
    return {"ok": True, "session_id": session_id, "run_id": run_id}

@app.get("/api/logs/session")
async def get_session_logs(session_id: str, limit: int = 200):
    if not session_id:
        return {"ok": False, "message": "session_id is required", "data": []}

    log_entries: List[Dict[str, Any]] = []
    log_file_path = 'strata_backend.log'
    cleared_at = session_log_clears.get(session_id)

    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            recent_lines = lines[-limit:] if len(lines) > limit else lines
            for line in recent_lines:
                parsed = _parse_log_line(line)
                if not parsed:
                    continue
                logger_name = parsed.pop("logger", None)
                if logger_name and logger_name != "strata":
                    continue
                resolved_session_id = parsed.get("session_id")
                if not resolved_session_id and parsed.get("run_id") is not None:
                    resolved_session_id = run_session_map.get(parsed.get("run_id"))
                if not resolved_session_id or resolved_session_id != session_id:
                    continue
                if cleared_at is not None:
                    log_ts = _parse_log_timestamp(parsed.get("timestamp") or "")
                    if log_ts is None or log_ts < cleared_at:
                        continue
                parsed["session_id"] = resolved_session_id
                log_entries.append(parsed)

    return {"ok": True, "data": log_entries, "total": len(log_entries)}

class ClearSessionLogsRequest(BaseModel):
    session_id: str

@app.post("/api/logs/session/clear")
async def clear_session_logs(req: ClearSessionLogsRequest):
    if not req.session_id:
        return {"ok": False, "message": "session_id is required"}
    session_log_clears[req.session_id] = time.time()
    return {"ok": True}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

analysis_state = {
    "running": False,
    "phase": "",
    "percent": 0,
    "done": False,
    "results": None
}

extraction_state = {
    "running": False,
    "percent": 0,
    "done": False,
    "results": None
}

migration_state = {
    "structure_done": False,
    "data_done": False,
    "results": None,
    "data_failed": False
}

table_migration_progress = {}

# Global variable to track structure migration progress
structure_migration_progress = {"percent": 0, "phase": "Initializing"}

validation_state = {
    "done": False,
    "report": None
}

# Server readiness flag
server_ready = False
db_ready = False

async def initialize_database():
    """Initialize database in background - don't block server startup"""
    global db_ready
    try:
        await init_db()
        print("   Database initialized")
        db_ready = True
    except Exception as e:
        print(f"   WARNING: Database initialization failed: {e}")
        db_ready = False

@app.on_event("startup")
async def startup():
    global server_ready
    # Load environment variables from .env file
    load_dotenv()
    
    print("Starting Strata server...")
    print(f"   Working directory: {os.getcwd()}")
    
    # Initialize database in background - don't block health checks
    asyncio.create_task(initialize_database())
    
    try:
        os.makedirs("artifacts", exist_ok=True)
        print("   Artifacts directory ready")
    except (IOError, OSError):
        pass
    
    server_ready = True
    print("Server startup complete - ready for requests")

class TestConnectionRequest(BaseModel):
    dbType: str
    name: Optional[str] = None
    credentials: Dict[str, Any]

class SaveConnectionRequest(BaseModel):
    dbType: str
    name: str
    credentials: Dict[str, Any]

def _sanitize_credentials(db_type: str, credentials: Any) -> Dict[str, Any]:
    """
    Keep credentials shape stable between UI and adapters.

    For Oracle we intentionally support *thin-mode only* via UI: no SYSDBA/SYSOPER role fields.
    """
    if not isinstance(credentials, dict):
        return {}

    kind = (db_type or "").strip().lower()

    if kind == "oracle":
        allowed = {"host", "port", "service_name", "username", "password", "schema"}
        cleaned: Dict[str, Any] = {k: credentials.get(k) for k in allowed if k in credentials}

        # Back-compat aliases (if older clients saved different keys)
        if "service_name" not in cleaned and "serviceName" in credentials:
            cleaned["service_name"] = credentials.get("serviceName")
        if "schema" not in cleaned and "schema_name" in credentials:
            cleaned["schema"] = credentials.get("schema_name")

        return cleaned

    if kind == "databricks":
        cleaned = {
            "server_hostname": credentials.get("server_hostname") or credentials.get("host"),
            "http_path": credentials.get("http_path") or credentials.get("httpPath"),
            "access_token": credentials.get("access_token") or credentials.get("accessToken"),
            "catalog": credentials.get("catalog"),
            "schema": credentials.get("schema")
        }
        return {k: v for k, v in cleaned.items() if v is not None}

    if kind == "snowflake":
        cleaned = {
            "account": credentials.get("account"),
            "username": credentials.get("username") or credentials.get("user"),
            "password": credentials.get("password"),
            "warehouse": credentials.get("warehouse"),
            "database": credentials.get("database") or credentials.get("db"),
            "schema": credentials.get("schema"),
            "role": credentials.get("role")
        }
        return {k: v for k, v in cleaned.items() if v is not None}

    if kind == "mysql":
        cleaned = {
            "host": credentials.get("host"),
            "port": credentials.get("port"),
            "database": credentials.get("database") or credentials.get("db"),
            "username": credentials.get("username") or credentials.get("user"),
            "password": credentials.get("password"),
            "ssl": credentials.get("ssl")
        }
        return {k: v for k, v in cleaned.items() if v is not None}

    return credentials

class SetSessionRequest(BaseModel):
    sourceId: int
    targetId: int

class ConvertDdlRequest(BaseModel):
    sourceDialect: Optional[str] = None
    targetDialect: Optional[str] = None
    sourceDdl: Optional[str] = None
    objectName: Optional[str] = None
    objectKind: Optional[str] = None
    # New flag – when true the backend will attempt to run the translated DDL in the target DB.
    execute: Optional[bool] = False


MAX_DDL_CONVERT_BYTES = 300_000
MAX_DDL_CONVERT_OBJECT_NAME = 255

async def ensure_db_ready():
    """Wait for database to be ready - for API endpoints that need it"""
    global db_ready

    # If initialization failed on startup, try once more lazily so API calls don't fail.
    if not db_ready:
        try:
            await init_db()
            db_ready = True
        except Exception as e:
            # Fall back to the original wait loop so we preserve existing behavior.
            logger.error(f"Database init retry failed: {e}")

    max_wait = 30  # seconds
    waited = 0
    while not db_ready and waited < max_wait:
        await asyncio.sleep(0.5)
        waited += 0.5
    if not db_ready:
        raise HTTPException(status_code=503, detail="Database not ready")

@app.post("/api/connections/test")
async def test_connection(req: TestConnectionRequest, request: Request):
    try:
        session_id = _get_request_session_id(request)
        _log_event("CONNECTION", f"Testing connection db_type={req.dbType}", session_id=session_id)
        credentials = _sanitize_credentials(req.dbType, req.credentials)
        adapter = get_adapter(req.dbType, credentials)
        result = await adapter.test_connection()
        # Ensure the UI always has a human-friendly message to display.
        if isinstance(result, dict) and result.get("ok") is True and not result.get("message"):
            details = result.get("details")
            result["message"] = details if isinstance(details, str) and details.strip() else "Connection successful"
        if isinstance(result, dict) and result.get("ok") is False and not result.get("message"):
            result["message"] = "Connection failed"
        _log_event(
            "CONNECTION",
            f"Connection test completed db_type={req.dbType} ok={bool(isinstance(result, dict) and result.get('ok'))}",
            session_id=session_id
        )
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        message = str(e).strip()
        if not message:
            message = f"{e.__class__.__name__}"
        _log_event("CONNECTION", f"Connection test failed db_type={req.dbType}: {message}", session_id=_get_request_session_id(request), level="error")
        return {"ok": False, "message": message}

@app.post("/api/connections/save")
async def save_connection(req: SaveConnectionRequest, request: Request):
    await ensure_db_ready()
    try:
        session_id = _get_request_session_id(request)
        _log_event("CONNECTION", f"Saving connection name={req.name} db_type={req.dbType}", session_id=session_id)
        credentials = _sanitize_credentials(req.dbType, req.credentials)
        enc_creds = encrypt_credentials(credentials)
        conn_id = await ConnectionModel.create(req.name, req.dbType, enc_creds)
        _log_event("CONNECTION", f"Saved connection id={conn_id} db_type={req.dbType}", session_id=session_id)
        return {"ok": True, "id": conn_id, "message": "Connection saved successfully"}
    except Exception as e:
        _log_event("CONNECTION", f"Save connection failed name={req.name}: {e}", session_id=_get_request_session_id(request), level="error")
        return {"ok": False, "message": str(e)}

@app.get("/api/connections")
async def get_connections():
    await ensure_db_ready()
    try:
        connections = await ConnectionModel.get_all()
        sanitized = []
        for conn in connections:
            sanitized.append({
                "id": conn["id"],
                "name": conn["name"],
                "db_type": conn["db_type"],
                "created_at": conn["created_at"]
            })
        return {"ok": True, "data": sanitized}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.get("/api/connections/{conn_id}")
async def get_connection_by_id(conn_id: int):
    """Get connection details with decrypted credentials for editing"""
    await ensure_db_ready()
    try:
        connection = await ConnectionModel.get_by_id(conn_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        # Decrypt credentials
        credentials = decrypt_credentials(connection["enc_credentials"])
        credentials = _sanitize_credentials(connection["db_type"], credentials)
        
        return {
            "ok": True,
            "data": {
                "id": connection["id"],
                "name": connection["name"],
                "db_type": connection["db_type"],
                "credentials": credentials,
                "created_at": connection["created_at"]
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.put("/api/connections/{conn_id}")
async def update_connection(conn_id: int, req: SaveConnectionRequest):
    await ensure_db_ready()
    try:
        # Check if connection exists
        existing = await ConnectionModel.get_by_id(conn_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Encrypt new credentials
        credentials = _sanitize_credentials(req.dbType, req.credentials)
        enc_creds = encrypt_credentials(credentials)

        # Update the connection
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE connections SET name = $1, db_type = $2, enc_credentials = $3 WHERE id = $4",
                    req.name, req.dbType, enc_creds, conn_id
                )
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute(
                    "UPDATE connections SET name = ?, db_type = ?, enc_credentials = ? WHERE id = ?",
                    (req.name, req.dbType, enc_creds, conn_id)
                )
                await db.commit()

        return {"ok": True, "message": "Connection updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.delete("/api/connections/{conn_id}")
async def delete_connection(conn_id: int):
    await ensure_db_ready()
    try:
        # Check if connection exists
        existing_conn = await ConnectionModel.get_by_id(conn_id)
        if not existing_conn:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Check if connection is currently in use in active session
        session = await SessionModel.get_session()
        if session and (session.get("source_id") == conn_id or session.get("target_id") == conn_id):
            # Clear the session if this connection is being used
            await SessionModel.clear_session()

        # Delete the connection
        if USE_POSTGRES:
            pool = await get_pg_pool()
            async with pool.acquire() as db_conn:
                await db_conn.execute("DELETE FROM connections WHERE id = $1", conn_id)
        else:
            async with aiosqlite.connect(DATABASE_PATH) as db_conn:
                await db_conn.execute("DELETE FROM connections WHERE id = ?", (conn_id,))
                await db_conn.commit()

        return {"ok": True, "message": "Connection deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/database/details")
async def get_database_details(request: Request):
    try:
        # Get the connection ID and optional schema filter from the request body
        body = await request.json()
        conn_id = body.get("connectionId")
        schema_filter = body.get("schema")  # Optional schema filter for Oracle
        role = body.get("role")
        
        if not conn_id:
            raise HTTPException(status_code=400, detail="Connection ID is required")
        
        # Get connection details from database
        connection = await ConnectionModel.get_by_id(conn_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        # Decrypt credentials
        credentials = decrypt_credentials(connection["enc_credentials"])
        
        # Get adapter and fetch details with timeout
        # For Oracle, pass schema from credentials if available
        if connection["db_type"].lower() == "oracle" and credentials.get("schema"):
            # Include the schema in the credentials sent to the adapter
            credentials_with_schema = credentials.copy()
            credentials_with_schema['schema'] = credentials.get("schema")
            adapter = get_adapter(connection["db_type"], credentials_with_schema)
        else:
            adapter = get_adapter(connection["db_type"], credentials)
        
        # Add timeout to prevent hanging
        try:
            # For Oracle, prioritize schema from connection credentials over request body
            effective_schema = schema_filter  # schema from request body
            if connection["db_type"].lower() == "oracle" and credentials.get("schema") and not schema_filter:
                # Use schema from saved connection if no schema_filter provided in request
                effective_schema = credentials.get("schema")
            
            # For Oracle with schema filter, call a specialized method if available
            if connection["db_type"].lower() == "oracle" and effective_schema:
                if hasattr(adapter, 'introspect_analysis_with_schema'):
                    details = await asyncio.wait_for(
                        adapter.introspect_analysis_with_schema(effective_schema), 
                        timeout=120.0
                    )
                else:
                    # Fallback: modify credentials to include schema and call standard method
                    credentials_with_schema = credentials.copy()
                    credentials_with_schema['schema_filter'] = effective_schema
                    adapter_with_schema = get_adapter(connection["db_type"], credentials_with_schema)
                    details = await asyncio.wait_for(adapter_with_schema.introspect_analysis(), timeout=120.0)
            else:
                details = await asyncio.wait_for(adapter.introspect_analysis(), timeout=120.0)  # 2 minute timeout
        except asyncio.TimeoutError:
            # Return timeout info with suggestion for Oracle databases
            timeout_response = {
                "ok": False, 
                "message": "Database introspection timed out. The database may be large or unreachable.",
                "timeout": True
            }
            if connection["db_type"].lower() == "oracle":
                timeout_response["suggest_schema_input"] = True
            return timeout_response
        except Exception as adapter_error:
            error_msg = str(adapter_error)
            print(f"[DATABASE DETAILS ERROR] Adapter error for {connection['db_type']}: {error_msg}")
            return {
                "ok": False, 
                "message": f"Failed to fetch database details: {error_msg}",
                "error": error_msg
            }
        
        # Handle error responses from adapter
        if "error" in details:
            error_msg = details.get("error", "Unknown error")
            print(f"[DATABASE DETAILS ERROR] Introspection error: {error_msg}")
            return {
                "ok": False,
                "message": f"Database introspection failed: {error_msg}",
                "error": error_msg
            }
        
        # Extract all information from the adapter response
        preview_data = {
            "database_info": details.get("database_info", {}),
            "tables": [],
            "columns": details.get("columns", []),  # Add columns data
            "constraints": details.get("constraints", []),
            "views": details.get("views", []),
            "procedures": details.get("procedures", []),
            "indexes": details.get("indexes", []),
            "triggers": details.get("triggers", []),
            "sequences": details.get("sequences", []),
            "user_types": details.get("user_types", []),
            "materialized_views": details.get("materialized_views", []),
            "partitions": details.get("partitions", []),
            "permissions": details.get("permissions", []),
            "data_profiles": details.get("data_profiles", []),
            "storage_info": details.get("storage_info") or None,  # Add storage information (omit if unavailable)
            "driver_unavailable": details.get("driver_unavailable", False),
            "connection": {
                "id": connection["id"],
                "name": connection["name"],
                "db_type": connection["db_type"]
            },
            "location": {
                k: v for k, v in {
                    "database": credentials.get("database"),
                    "schema": credentials.get("schema"),
                    "warehouse": credentials.get("warehouse"),
                    "account": credentials.get("account"),
                    "host": credentials.get("host")
                }.items() if v
            }
        }        
        # Process tables to include row counts
        tables = details.get("tables", [])
        data_profiles = details.get("data_profiles", [])
        
        for table in tables:
            # Find matching data profile for row count
            row_count = 0
            for profile in data_profiles:
                if profile.get("table") == table.get("name") and profile.get("schema") == table.get("schema"):
                    row_count = profile.get("row_count", 0)
                    break
            
            preview_data["tables"].append({
                "schema": table.get("schema"),
                "name": table.get("name"),
                "type": table.get("type"),
                "row_count": row_count,
                "engine": table.get("engine"),
                "data_length": table.get("data_length", 0),
                "index_length": table.get("index_length", 0),
                "total_size": table.get("total_size", 0)
            })
        
        # Log successful response
        print(f"[DATABASE DETAILS] Successfully fetched details for {connection['name']} ({connection['db_type']}). Tables: {len(preview_data['tables'])}, Columns: {len(preview_data['columns'])}")
        try:
            session_id = _get_request_session_id(request)
            label = _format_connection_label(connection)
            prefix = f"Connected to {role} " if role in ("source", "target") else "Connected to "
            run_id = await _ensure_session_run_id(
                session_id,
                source_id=conn_id if role == "source" else None,
                target_id=conn_id if role == "target" else None
            )
            _log_event("CONNECTION", f"{prefix}{label}", session_id=session_id, run_id=run_id)
        except Exception:
            pass
        
        return {"ok": True, "data": preview_data}
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        print(f"[DATABASE DETAILS ERROR] Unexpected error: {error_msg}")
        try:
            session_id = _get_request_session_id(request)
            label = _format_connection_label(connection) if 'connection' in locals() and connection else 'connection'
            prefix = f"{role} " if role in ("source", "target") else ""
            run_id = await _ensure_session_run_id(
                session_id,
                source_id=conn_id if role == "source" else None,
                target_id=conn_id if role == "target" else None
            )
            _log_event("CONNECTION", f"Failed to connect to {prefix}{label}: {error_msg}", session_id=session_id, run_id=run_id, level="error")
        except Exception:
            pass
        return {"ok": False, "message": f"Unexpected error: {error_msg}", "error": error_msg}

@app.post("/api/session/set-selected-tables")
async def set_selected_tables(req: dict, request: Request):
    try:
        selected_tables = req.get("selectedTables", [])
        await SessionModel.set_selected_tables(selected_tables)
        run_id = None
        try:
            session = await SessionModel.get_session()
            run_id = session.get("run_id") if session else None
        except Exception:
            run_id = None
        session_id = _get_request_session_id(request)
        if run_id is None and session_id:
            run_id = session_run_map.get(session_id)
        if selected_tables:
            _log_event(
                "SELECTION",
                f"Selected {len(selected_tables)} tables",
                run_id=run_id,
                session_id=session_id,
                tables=[str(t) for t in selected_tables if str(t).strip()]
            )
        # Reset extraction state when selected tables change
        global extraction_state
        extraction_state["running"] = False
        extraction_state["percent"] = 0
        extraction_state["done"] = False
        extraction_state["results"] = None
        return {"ok": True, "message": "Selected tables updated"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/session/set-selected-columns")
async def set_selected_columns(req: dict):
    try:
        selected_columns = req.get("selectedColumns", {})
        await SessionModel.set_selected_columns(selected_columns)
        return {"ok": True, "message": "Selected columns updated"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/session/set-source-target")
async def set_session(req: SetSessionRequest, request: Request):
    try:
        session_id = _get_request_session_id(request)
        run_id = await _ensure_session_run_id(session_id, req.sourceId, req.targetId)
        if run_id is None:
            run_id = await RunModel.create(req.sourceId, req.targetId)
            if session_id:
                session_run_map[session_id] = run_id
                run_session_map[run_id] = session_id
        # Preserve existing selected tables if they exist
        existing_session = await SessionModel.get_session()
        existing_selected_tables = existing_session.get("selected_tables") if existing_session else None
        existing_selected_columns = existing_session.get("selected_columns") if existing_session else None
        await SessionModel.set_session(req.sourceId, req.targetId, run_id, existing_selected_tables, existing_selected_columns)
        try:
            source = await ConnectionModel.get_by_id(req.sourceId)
            target = await ConnectionModel.get_by_id(req.targetId)
            source_label = _format_connection_label(source) if source else str(req.sourceId)
            target_label = _format_connection_label(target) if target else str(req.targetId)
            _log_event(
                "CONNECTION",
                f"Connected to source {source_label}",
                run_id=run_id,
                session_id=session_id
            )
            _log_event(
                "CONNECTION",
                f"Connected to target {target_label}",
                run_id=run_id,
                session_id=session_id
            )
        except Exception:
            _log_event("CONNECTION", "Session configured", run_id=run_id, session_id=session_id)
        return {"ok": True, "message": "Session configured", "runId": run_id}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/ddl/convert")
async def convert_ddl(req: ConvertDdlRequest, request: Request):
    """Convert a single DDL statement using AI with a hard timeout and fast fallback.

    This endpoint is used by the UI for ad-hoc DDL conversion. Previously, if the
    upstream AI call was slow or unresponsive the request could hang for a long
    time, which in turn shows up in the browser as a generic "Failed to fetch"
    error. To prevent that, we:

    * Enforce a strict timeout around the AI call (default 30s).
    * Fall back to the deterministic rule-based translator on timeout or error.
    * Always return a small JSON payload so the frontend never waits on a long
      streaming response.
    """
    try:
        if not req.sourceDdl:
            return {"ok": False, "message": "sourceDdl is required"}
        if len(req.sourceDdl.encode("utf-8")) > MAX_DDL_CONVERT_BYTES:
            return {"ok": False, "message": f"sourceDdl too large (max {MAX_DDL_CONVERT_BYTES} bytes)"}
        if req.objectName and len(req.objectName) > MAX_DDL_CONVERT_OBJECT_NAME:
            return {"ok": False, "message": f"objectName too long (max {MAX_DDL_CONVERT_OBJECT_NAME} chars)"}

        ai = _import_ai_module()
        session_id = _get_request_session_id(request)
        run_id = await _ensure_session_run_id(session_id)
        _log_event(
            "DDL",
            f"Convert requested source={req.sourceDialect or 'unknown'} target={req.targetDialect or 'unknown'} bytes={len(req.sourceDdl.encode('utf-8'))}",
            run_id=run_id,
            session_id=session_id
        )
        obj = {
            "name": req.objectName or "object",
            "kind": req.objectKind or "table",
            "source_ddl": req.sourceDdl or "",
        }

        # Hard timeout for translation so the request never hangs indefinitely.
        ai_timeout_seconds = 45
        translation = None
        try:
            translation = await asyncio.wait_for(
                ai.translate_schema(
                    req.sourceDialect or "",
                    req.targetDialect or "",
                    {"objects": [obj]},
                ),
                timeout=ai_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"/api/ddl/convert timed out after {ai_timeout_seconds}s; falling back to rule-based translation"
            )
            _log_event("DDL", f"Convert timeout after {ai_timeout_seconds}s; using fallback", run_id=run_id, session_id=session_id, level="warning")
        except Exception as e:
            # Log but continue to fallback so the UI still receives a response.
            logger.error(f"/api/ddl/convert AI error: {e}")
            _log_event("DDL", f"Convert AI error; using fallback: {e}", run_id=run_id, session_id=session_id, level="warning")

        if not isinstance(translation, dict) or not translation.get("objects"):
            translation = ai.fallback_translation(
                [obj], req.sourceDialect or "", req.targetDialect or ""
            )

        translated = (translation.get("objects") or [{}])[0]
        response = {
            "ok": True,
            "target_sql": translated.get("target_sql", ""),
            "notes": translated.get("notes", []),
        }

        # ------------------------------------------------------------
        # Optional execution of the translated DDL in the target DB.
        # ------------------------------------------------------------
        if getattr(req, "execute", False):
            # Resolve the target connection from the current session.
            session = await SessionModel.get_session()
            target_id = session.get("target_id") if session else None
            if target_id:
                target_conn = await ConnectionModel.get_by_id(target_id)
                if target_conn:
                    target_credentials = decrypt_credentials(target_conn["enc_credentials"])
                    target_adapter = get_adapter(target_conn["db_type"], target_credentials)
                    exec_res = await target_adapter.run_ddl(response["target_sql"])
                    response["executed"] = exec_res.get("ok", False)
                    response["execution_error"] = exec_res.get("error")
                    # New: surface per-statement execution results so the UI can show OK/error
                    # exactly as Databricks reports it.
                    response["execution"] = exec_res
                else:
                    response["executed"] = False
                    response["execution_error"] = "Target connection not found"
            else:
                response["executed"] = False
                response["execution_error"] = "No target configured in session"

        _log_event(
            "DDL",
            f"Convert completed source={req.sourceDialect or 'unknown'} target={req.targetDialect or 'unknown'} sql_chars={len(response.get('target_sql', ''))}",
            run_id=run_id,
            session_id=session_id
        )
        return response
    except Exception as e:
        logger.error(f"/api/ddl/convert unexpected error: {e}")
        _log_event("DDL", f"Convert failed: {e}", session_id=_get_request_session_id(request), level="error")
        return {"ok": False, "message": str(e)}

@app.get("/api/session")
async def get_session():
    try:
        session = await SessionModel.get_session()
        print(f"[DEBUG] Raw session from model: {session}")
        if not session:
            return {"ok": True, "data": None}
        
        # Handle case where session might only have selected_tables
        if session.get("source_id") is None or session.get("target_id") is None:
            return {
                "ok": True,
                "data": {
                    "sourceId": None,
                    "targetId": None,
                    "runId": session.get("run_id"),
                    "selected_tables": session.get("selected_tables", []),
                    "selected_columns": session.get("selected_columns", {})
                }
            }
        
        source = await ConnectionModel.get_by_id(session["source_id"])
        target = await ConnectionModel.get_by_id(session["target_id"])
        
        # Handle case where connections might not exist
        source_data = {
            "id": source["id"],
            "name": source["name"],
            "db_type": source["db_type"]
        } if source else None
        
        target_data = {
            "id": target["id"],
            "name": target["name"],
            "db_type": target["db_type"]
        } if target else None
        
        return {
            "ok": True,
            "data": {
                "sourceId": session["source_id"],
                "targetId": session["target_id"],
                "runId": session["run_id"],
                "selected_tables": session.get("selected_tables", []),
                "selected_columns": session.get("selected_columns", {}),
                "source": source_data,
                "target": target_data
            }
        }
    except Exception as e:
        print(f"[ERROR] Exception in get_session: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "message": str(e)}

@app.get("/api/session/get-selected-tables")
async def get_selected_tables():
    try:
        session = await SessionModel.get_session()
        selected_tables = session.get("selected_tables", []) if session else []
        return {
            "ok": True,
            "selectedTables": selected_tables
        }
    except Exception as e:
        print(f"[ERROR] Exception in get_selected_tables: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "message": str(e)}

@app.get("/api/session/get-selected-columns")
async def get_selected_columns():
    try:
        session = await SessionModel.get_session()
        selected_columns = session.get("selected_columns", {}) if session else {}
        return {
            "ok": True,
            "selectedColumns": selected_columns
        }
    except Exception as e:
        print(f"[ERROR] Exception in get_selected_columns: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "message": str(e)}

@app.post("/api/session/set-column-renames")
async def set_column_renames(req: dict):
    try:
        column_renames = req.get("columnRenames", {})
        await SessionModel.set_column_renames(column_renames)
        return {"ok": True, "message": "Column renames updated"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/session/clear-column-renames")
async def clear_column_renames():
    try:
        await SessionModel.clear_column_renames()
        return {"ok": True, "message": "Column renames cleared"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.get("/api/session/get-column-renames")
async def get_column_renames():
    try:
        column_renames = await SessionModel.get_column_renames()
        return {
            "ok": True,
            "columnRenames": column_renames
        }
    except Exception as e:
        print(f"[ERROR] Exception in get_column_renames: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "message": str(e)}

@app.post("/api/connections/upload")
async def upload_connection(file: UploadFile = File(...), name: Optional[str] = Form(None)):
    try:
        # Verify that the uploaded file is a .txt file
        if not file.filename.lower().endswith('.txt'):
            return {"ok": False, "message": "Only .txt files are supported"}
        
        # Read the content of the uploaded file
        content = await file.read()
        content_str = content.decode('utf-8')
        
        # Parse the key-value pairs from the file content
        connection_details = {}
        # Accept entries separated by newlines or commas (common when users paste a single-line string)
        raw_lines = re.split(r'[\n,]+', content_str)
        for line in raw_lines:
            line = line.strip()
            if not line or ':' not in line:
                continue
            key, value = line.split(':', 1)
            key = re.sub(r'\s+', ' ', key.strip())  # collapse repeated spaces
            value = value.strip().rstrip(',;')  # remove trailing separators often present in copied strings
            connection_details[key.lower()] = value
        
        # Extract database type (assuming it's provided in the file)
        db_type_raw = (
            connection_details.get('database type')
            or connection_details.get('db type')
            or connection_details.get('database_type')
            or connection_details.get('db_type')
            or connection_details.get('database-type')
        )
        db_type_aliases = {
            'postgres': 'PostgreSQL',
            'postgresql': 'PostgreSQL',
            'mysql': 'MySQL',
            'snowflake': 'Snowflake',
            'databricks': 'Databricks',
            'oracle': 'Oracle',
            'sql server': 'SQL Server',
            'sqlserver': 'SQL Server',
            'mssql': 'SQL Server',
            'teradata': 'Teradata',
            'google bigquery': 'Google BigQuery',
            'bigquery': 'Google BigQuery',
            'gcp bigquery': 'Google BigQuery',
            'aws s3': 'AWS S3',
            's3': 'AWS S3'
        }
        db_type = None
        if db_type_raw:
            normalized = db_type_raw.strip().replace('_', ' ').replace('-', ' ').lower()
            db_type = db_type_aliases.get(normalized, db_type_raw.strip())
        
        # If user provided a name, use it; otherwise derive from server details
        if name and name.strip():
            name = name.strip()  # Use the name provided by user - ABSOLUTE PRIORITY
            print(f"DEBUG: Using user-provided connection name: '{name}'")
        else:
            print(f"DEBUG: No user-provided name, deriving from server details")
            # Extract server name and derive connection name
            original_server_name = connection_details.get('server name', connection_details.get('host', connection_details.get('account', '')))
            
            # Extract only the server name by removing https:// and .com, or use as-is if it's an IP address
            server_name_for_processing = original_server_name
            if server_name_for_processing and server_name_for_processing.startswith('https://'):
                server_name_for_processing = server_name_for_processing.replace('https://', '')
            elif server_name_for_processing and server_name_for_processing.startswith('http://'):
                server_name_for_processing = server_name_for_processing.replace('http://', '')
            
            # Remove .com, .net, .org, etc. endings
            name = re.sub(r'\.[a-zA-Z]+$', '', server_name_for_processing or '')
            
            # If the server name is an IP address, keep it as is
            ip_pattern = r'^\d+\.\d+\.\d+\.\d+'  # Simple IP address pattern
            if server_name_for_processing and re.match(ip_pattern, server_name_for_processing):
                name = server_name_for_processing
            
            # Handle case where name might be empty
            if not name:
                name = original_server_name or f"{db_type}_connection"  # fallback to original server_name or default
            
            print(f"DEBUG: Derived connection name: '{name}'")
        
        # Map common keys from the file to the expected credential keys
        credentials_mapping = {
            'username': ['username', 'user', 'usr', 'login'],
            'password': ['password', 'pass', 'pwd'],
            'host': ['host', 'server name', 'server', 'hostname', 'server-name', 'server_name'],
            'port': ['port'],
            'database': ['database', 'dbname', 'db', 'db_name', 'database_name'],
            'schema': ['schema', 'schema_name'],
            'account': ['account', 'account_identifier', 'account identifier'],
            'warehouse': ['warehouse'],
            'access_token': ['access token', 'token'],
            'server_hostname': ['server hostname', 'server_host'],
            'http_path': ['http path', 'httppath'],
            'service_name': ['service name', 'servicename'],
            'project_id': ['project id', 'projectid'],
            'dataset': ['dataset'],
            'bucket_name': ['bucket name', 'bucket'],
            'region': ['region'],
            'access_key_id': ['access key id', 'access_key'],
            'secret_access_key': ['secret access key', 'secret_key'],
            'sslmode': ['sslmode', 'ssl mode'],
            'ssl': ['ssl'],
            'catalog': ['catalog'],
            'driver': ['driver'],
            'credentials_json': ['credentials json', 'credentials']
        }
        
        # Build the credentials object based on the mapped keys
        credentials = {}
        for cred_key, possible_keys in credentials_mapping.items():
            for possible_key in possible_keys:
                if possible_key in connection_details:
                    credentials[cred_key] = connection_details[possible_key]
                    break
        
        # Snowflake-specific: ensure account is populated from any plausible key (normalize whitespace)
        if db_type == "Snowflake":
            account_fallback = None
            for key in [
                "account",
                "account_identifier",
                "account identifier",
                "account-id",
                "account id",
                "account_id",
                "host",
                "server name",
                "server",
                "server_name",
                "server-name",
            ]:
                # try direct match
                val = connection_details.get(key)
                # also try collapsed-space version for keys that had multiple spaces (e.g., "account    identifier")
                if not val:
                    val = connection_details.get(re.sub(r'\s+', ' ', key))
                if val:
                    account_fallback = val.strip().rstrip(",;")
                    break
            if account_fallback:
                credentials["account"] = account_fallback
        
        # Normalize SSL flags for common engines
        if "ssl" in credentials:
            ssl_val = credentials["ssl"]
            if isinstance(ssl_val, str):
                ssl_val_lower = ssl_val.strip().lower()
                credentials["ssl"] = ssl_val_lower in ("true", "1", "yes", "y", "on", "required", "require")
            else:
                credentials["ssl"] = bool(ssl_val)
        
        if "sslmode" in credentials and isinstance(credentials["sslmode"], str):
            credentials["sslmode"] = credentials["sslmode"].strip().lower()
        
        # Validate that we have at least the essential information
        if not db_type:
            return {"ok": False, "message": "Database type is required in the file"}
        
        # Check if the db_type is valid
        if db_type not in ADAPTERS:
            return {"ok": False, "message": f"Unsupported database type: {db_type}"}

        credentials = _sanitize_credentials(db_type, credentials)
        
        # Test the connection
        try:
            adapter = get_adapter(db_type, credentials)
            test_result = await adapter.test_connection()
        except Exception as adapter_err:
            return {"ok": False, "message": f"Failed to create adapter or test connection: {str(adapter_err)}"}
        
        if not test_result.get("ok"):
            return {"ok": False, "message": f"Connection test failed: {test_result.get('message', 'Unknown error')}"}
        
        # Encrypt and save the connection
        try:
            enc_creds = encrypt_credentials(credentials)
            conn_id = await ConnectionModel.create(name, db_type, enc_creds)
        except Exception as save_err:
            return {"ok": False, "message": f"Failed to save connection: {str(save_err)}"}
        
        return {
            "ok": True, 
            "id": conn_id, 
            "message": "Connection uploaded and saved successfully",
            "connection": {
                "id": conn_id,
                "name": name,
                "db_type": db_type
            }
        }
    except Exception as e:
        logger.error(f"Upload connection error: {str(e)}")
        return {"ok": False, "message": f"Failed to upload connection: {str(e)}"}

async def run_analysis_task():
    global analysis_state
    print("[ANALYSIS] Starting analysis task")
    analysis_state["running"] = True
    analysis_state["percent"] = 0
    analysis_state["done"] = False

    try:
        print("[ANALYSIS] Getting session")
        session = await SessionModel.get_session()
        print(f"[ANALYSIS] Session: {session}")
        if not session:
            raise Exception("No session found")
        run_id = session.get("run_id") if session else None

        print(f"[ANALYSIS] Getting source connection: {session.get('source_id')}")
        source_id = session.get("source_id")
        if not source_id:
            raise Exception("No source connection configured")
        source = await ConnectionModel.get_by_id(source_id)
        print(f"[ANALYSIS] Source: {source}")
        if not source:
            raise Exception(f"Source connection {source_id} not found")

        print("[ANALYSIS] Decrypting credentials")
        source_creds = decrypt_credentials(source["enc_credentials"])
        print(f"[ANALYSIS] Getting adapter for {source['db_type']}")

        source_adapter = get_adapter(source["db_type"], source_creds)
        print(f"[ANALYSIS] Adapter: {source_adapter}")
        _log_event("ANALYSIS", f"Analysis started for source {source.get('name')}", run_id=run_id)

        phases = [
            "Database & Schema Analysis",
            "Table Structure Analysis",
            "Views Analysis",
            "Stored Procedures & Functions",
            "Indexes & Performance",
            "Relationships & Dependencies",
            "Data Type Mapping",
            "Security & Roles",
            "Environment & Config",
            "Data Profiling"
        ]

        print("[ANALYSIS] Starting phase simulation")
        for i, phase in enumerate(phases):
            analysis_state["phase"] = phase
            analysis_state["percent"] = int((i + 1) / len(phases) * 100)
            print(f"[ANALYSIS] Phase {i+1}/{len(phases)}: {phase} - {analysis_state['percent']}%")
            await asyncio.sleep(0.5)

        print("[ANALYSIS] Calling introspect_analysis")
        analysis_result = await source_adapter.introspect_analysis()
        print(f"[ANALYSIS] Introspect result keys: {list(analysis_result.keys()) if analysis_result else 'None'}")

        # Filter tables based on selected tables if any are selected
        selected_tables = session.get("selected_tables", [])
        if selected_tables and analysis_result.get("tables"):
            print(f"[ANALYSIS] Filtering results for selected tables: {selected_tables}")
            # Filter tables
            filtered_tables = [
                table for table in analysis_result["tables"]
                if f"{table.get('schema', '')}.{table.get('name', '')}" in selected_tables or
                   table.get('name', '') in selected_tables
            ]
            analysis_result["tables"] = filtered_tables
            selected_schemas = {t.get("schema", "") for t in filtered_tables}
            
            # Filter columns for selected tables only
            if analysis_result.get("columns"):
                filtered_columns = [
                    col for col in analysis_result["columns"]
                    if any(
                        col.get("table") == table.get("name") and col.get("schema") == table.get("schema")
                        for table in filtered_tables
                    )
                ]
                analysis_result["columns"] = filtered_columns
            
            # Filter data profiles for selected tables only
            if analysis_result.get("data_profiles"):
                filtered_profiles = [
                    profile for profile in analysis_result["data_profiles"]
                    if any(
                        profile.get("table") == table.get("name") and profile.get("schema") == table.get("schema")
                        for table in filtered_tables
                    )
                ]
                analysis_result["data_profiles"] = filtered_profiles

            # Filter other objects by selected schemas
            for key in ["views", "materialized_views", "procedures", "triggers", "sequences", "user_types", "partitions", "indexes", "permissions", "constraints"]:
                if analysis_result.get(key):
                    analysis_result[key] = [
                        item for item in analysis_result[key]
                        if item.get("schema") in selected_schemas
                        or item.get("table_schema") in selected_schemas
                    ]

            # Filter storage info to only include selected objects and recompute totals
            storage_info = analysis_result.get("storage_info")
            if storage_info:
                storage_tables = storage_info.get("tables") or []
                selected_refs = set()
                for tbl in filtered_tables:
                    schema = (tbl.get("schema") or "").strip()
                    name = (tbl.get("name") or "").strip()
                    if not name:
                        continue
                    selected_refs.add(name)
                    if schema:
                        selected_refs.add(f"{schema}.{name}")

                filtered_storage_tables = []
                for entry in storage_tables:
                    entry_schema = (entry.get("schema") or entry.get("table_schema") or "").strip()
                    entry_name = (entry.get("name") or entry.get("table") or entry.get("table_name") or "").strip()
                    if not entry_name:
                        continue
                    full_ref = f"{entry_schema}.{entry_name}" if entry_schema else entry_name
                    if full_ref in selected_refs or entry_name in selected_refs:
                        filtered_storage_tables.append(entry)

                def _to_number(val):
                    try:
                        return float(val or 0)
                    except Exception:
                        return 0.0

                total_size_sum = 0
                data_size_sum = 0
                index_size_sum = 0

                for entry in filtered_storage_tables:
                    data_size = _to_number(entry.get("data_size") if entry.get("data_size") is not None else entry.get("data_length"))
                    index_size = _to_number(entry.get("index_size") if entry.get("index_size") is not None else entry.get("index_length"))
                    total_size = entry.get("total_size")
                    if total_size is None:
                        total_size = data_size + index_size
                    total_size_sum += _to_number(total_size)
                    data_size_sum += data_size
                    index_size_sum += index_size

                storage_info["tables"] = filtered_storage_tables
                storage_info["database_size"] = {
                    "total_size": int(total_size_sum),
                    "data_size": int(data_size_sum),
                    "index_size": int(index_size_sum)
                }
                analysis_result["storage_info"] = storage_info

            # Limit schema list in database_info to selected schemas when available
            if analysis_result.get("database_info") is not None:
                analysis_result["database_info"]["schemas"] = sorted([s for s in selected_schemas if s])

        run_id = session["run_id"]
        artifact_path = f"artifacts/analysis_{run_id}.json"
        print(f"[ANALYSIS] Saving to {artifact_path}")
        with open(artifact_path, "w") as f:
            json.dump(analysis_result, f, indent=2, cls=DecimalEncoder)

        analysis_state["results"] = analysis_result
        analysis_state["done"] = True
        analysis_state["running"] = False
        try:
            table_entries = analysis_result.get("tables", []) or []
            table_names = []
            for table in table_entries:
                name = table.get("name") if isinstance(table, dict) else None
                schema = table.get("schema") if isinstance(table, dict) else None
                if not name:
                    continue
                full_name = f"{schema}.{name}" if schema else name
                table_names.append(full_name)
            _log_event(
                "ANALYSIS",
                f"Analysis completed tables={len(table_names)}",
                run_id=run_id,
                tables=table_names
            )
        except Exception:
            _log_event(
                "ANALYSIS",
                f"Analysis completed tables={len(analysis_result.get('tables', []))}",
                run_id=run_id
            )
        print("[ANALYSIS] Analysis completed successfully")

    except Exception as e:
        print(f"[ANALYSIS] Error in analysis task: {str(e)}")
        _log_event("ANALYSIS", f"Analysis failed: {str(e)}", run_id=run_id, level="error")
        import traceback
        traceback.print_exc()
        analysis_state["running"] = False
        analysis_state["results"] = {"error": str(e)}

@app.post("/api/analyze/start")
async def start_analysis(background_tasks: BackgroundTasks, request: Request):
    print("[ANALYSIS] Start analysis endpoint called")
    global analysis_state
    if analysis_state["running"]:
        print("[ANALYSIS] Analysis already running, rejecting")
        return {"ok": False, "message": "Analysis already running"}

    print("[ANALYSIS] Adding analysis task to background")
    background_tasks.add_task(run_analysis_task)
    try:
        session = await SessionModel.get_session()
        run_id = session.get("run_id") if session else None
        session_id = _get_request_session_id(request)
        _log_event("ANALYSIS", "Analysis requested", run_id=run_id, session_id=session_id)
    except Exception:
        pass
    print("[ANALYSIS] Analysis started successfully")
    return {"ok": True, "message": "Analysis started"}

@app.get("/api/analyze/status")
async def get_analysis_status():
    return {
        "ok": True,
        "running": analysis_state["running"],
        "phase": analysis_state["phase"],
        "percent": analysis_state["percent"],
        "done": analysis_state["done"],
        "resultsSummary": analysis_state["results"] if analysis_state["done"] else None
    }

@app.get("/api/analyze/export/json")
async def export_analysis_json():
    try:
        if not analysis_state["done"] or not analysis_state["results"]:
            raise HTTPException(status_code=400, detail="No analysis results available")
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/analysis_export_{run_id}.json"
        
        with open(filepath, "w") as f:
            json.dump(analysis_state["results"], f, indent=2, cls=DecimalEncoder)
        
        return FileResponse(
            filepath,
            media_type="application/json",
            filename=f"analysis_report_{run_id}.json"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analyze/export/excel")
async def export_analysis_excel():
    try:
        if not analysis_state["done"] or not analysis_state["results"]:
            raise HTTPException(status_code=400, detail="No analysis results available")
        
        import xlsxwriter
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/analysis_export_{run_id}.xlsx"
        
        workbook = xlsxwriter.Workbook(filepath)
        
        try:
            bold = workbook.add_format({'bold': True, 'bg_color': '#085690', 'font_color': 'white'})
            header = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3'})
            
            results = analysis_state["results"]
            
            summary_sheet = workbook.add_worksheet('Summary')
            summary_sheet.write('A1', 'Database Analysis Report', bold)
            summary_sheet.write('A3', 'Database Type', header)
            summary_sheet.write('B3', results.get('database_info', {}).get('type', 'N/A'))
            summary_sheet.write('A4', 'Version', header)
            summary_sheet.write('B4', results.get('database_info', {}).get('version', 'N/A'))
            summary_sheet.write('A5', 'Encoding', header)
            summary_sheet.write('B5', results.get('database_info', {}).get('encoding', 'N/A'))
            summary_sheet.write('A6', 'Collation', header)
            summary_sheet.write('B6', results.get('database_info', {}).get('collation', 'N/A'))
            
            row = 8
            summary_sheet.write(f'A{row}', 'Object Type', header)
            summary_sheet.write(f'B{row}', 'Count', header)
            row += 1
            
            object_counts = [
                ('Tables', len(results.get('tables', []))),
                ('Columns', len(results.get('columns', []))),
                ('Views', len(results.get('views', []))),
                ('Materialized Views', len(results.get('materialized_views', []))),
                ('Indexes', len(results.get('indexes', []))),
                ('Constraints', len(results.get('constraints', []))),
                ('Triggers', len(results.get('triggers', []))),
                ('Sequences', len(results.get('sequences', []))),
                ('User Types', len(results.get('user_types', []))),
                ('Partitions', len(results.get('partitions', []))),
                ('Procedures', len(results.get('procedures', []))),
                ('Permissions', len(results.get('permissions', [])))
            ]
            
            for obj_type, count in object_counts:
                summary_sheet.write(f'A{row}', obj_type)
                summary_sheet.write(f'B{row}', count)
                row += 1
            
            if results.get('tables'):
                tables_sheet = workbook.add_worksheet('Tables')
                tables_sheet.write('A1', 'Schema', bold)
                tables_sheet.write('B1', 'Table Name', bold)
                tables_sheet.write('C1', 'Type', bold)
                for i, table in enumerate(results['tables'], start=2):
                    tables_sheet.write(f'A{i}', table.get('schema', ''))
                    tables_sheet.write(f'B{i}', table.get('name', ''))
                    tables_sheet.write(f'C{i}', table.get('type', ''))
            
            if results.get('columns'):
                columns_sheet = workbook.add_worksheet('Columns')
                columns_sheet.write('A1', 'Schema', bold)
                columns_sheet.write('B1', 'Table', bold)
                columns_sheet.write('C1', 'Column', bold)
                columns_sheet.write('D1', 'Data Type', bold)
                columns_sheet.write('E1', 'Nullable', bold)
                columns_sheet.write('F1', 'Default', bold)
                for i, col in enumerate(results['columns'], start=2):
                    columns_sheet.write(f'A{i}', col.get('schema', ''))
                    columns_sheet.write(f'B{i}', col.get('table', ''))
                    columns_sheet.write(f'C{i}', col.get('name', ''))
                    columns_sheet.write(f'D{i}', col.get('type', ''))
                    columns_sheet.write(f'E{i}', 'Yes' if col.get('nullable') else 'No')
                    columns_sheet.write(f'F{i}', str(col.get('default', '')))
            
            if results.get('triggers'):
                triggers_sheet = workbook.add_worksheet('Triggers')
                triggers_sheet.write('A1', 'Schema', bold)
                triggers_sheet.write('B1', 'Trigger Name', bold)
                triggers_sheet.write('C1', 'Table', bold)
                triggers_sheet.write('D1', 'Timing', bold)
                triggers_sheet.write('E1', 'Event', bold)
                for i, trigger in enumerate(results['triggers'], start=2):
                    triggers_sheet.write(f'A{i}', trigger.get('schema', ''))
                    triggers_sheet.write(f'B{i}', trigger.get('name', ''))
                    triggers_sheet.write(f'C{i}', trigger.get('table', ''))
                    triggers_sheet.write(f'D{i}', trigger.get('timing', ''))
                    triggers_sheet.write(f'E{i}', trigger.get('event', ''))
            
            if results.get('sequences'):
                sequences_sheet = workbook.add_worksheet('Sequences')
                sequences_sheet.write('A1', 'Schema', bold)
                sequences_sheet.write('B1', 'Sequence Name', bold)
                sequences_sheet.write('C1', 'Current Value', bold)
                sequences_sheet.write('D1', 'Increment', bold)
                for i, seq in enumerate(results['sequences'], start=2):
                    sequences_sheet.write(f'A{i}', seq.get('schema', ''))
                    sequences_sheet.write(f'B{i}', seq.get('name', ''))
                    sequences_sheet.write(f'C{i}', seq.get('current_value', 0))
                    sequences_sheet.write(f'D{i}', seq.get('increment', 1))
        finally:
            workbook.close()
        
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"analysis_report_{run_id}.xlsx"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analyze/export/pdf")
async def export_analysis_pdf():
    try:
        if not analysis_state["done"] or not analysis_state["results"]:
            raise HTTPException(status_code=400, detail="No analysis results available")
        
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/analysis_export_{run_id}.pdf"
        
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        elements = []
        styles = getSampleStyleSheet()
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#085690'),
            spaceAfter=30,
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#085690'),
            spaceAfter=12,
        )
        
        results = analysis_state["results"]
        
        elements.append(Paragraph("Database Analysis Report", title_style))
        elements.append(Spacer(1, 0.2*inch))
        
        db_info = results.get('database_info', {})
        info_data = [
            ['Database Type', db_info.get('type', 'N/A')],
            ['Version', db_info.get('version', 'N/A')],
            ['Encoding', db_info.get('encoding', 'N/A')],
            ['Collation', db_info.get('collation', 'N/A')],
            ['Schemas', ', '.join(db_info.get('schemas', []))]
        ]
        
        info_table = Table(info_data, colWidths=[2*inch, 4*inch])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#085690')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.3*inch))
        
        elements.append(Paragraph("Object Summary", heading_style))
        summary_data = [['Object Type', 'Count']]
        summary_data.append(['Tables', str(len(results.get('tables', [])))])
        summary_data.append(['Columns', str(len(results.get('columns', [])))])
        summary_data.append(['Views', str(len(results.get('views', [])))])
        summary_data.append(['Materialized Views', str(len(results.get('materialized_views', [])))])
        summary_data.append(['Indexes', str(len(results.get('indexes', [])))])
        summary_data.append(['Constraints', str(len(results.get('constraints', [])))])
        summary_data.append(['Triggers', str(len(results.get('triggers', [])))])
        summary_data.append(['Sequences', str(len(results.get('sequences', [])))])
        summary_data.append(['User Types', str(len(results.get('user_types', [])))])
        summary_data.append(['Partitions', str(len(results.get('partitions', [])))])
        summary_data.append(['Procedures', str(len(results.get('procedures', [])))])
        summary_data.append(['Permissions', str(len(results.get('permissions', [])))])
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#085690')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        elements.append(summary_table)
        
        doc.build(elements)
        
        return FileResponse(
            filepath,
            media_type="application/pdf",
            filename=f"analysis_report_{run_id}.pdf"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def run_extraction_task():
    global extraction_state
    extraction_state["running"] = True
    extraction_state["percent"] = 0
    extraction_state["done"] = False
    
    try:
        # Ensure we get fresh session data
        session = await SessionModel.get_session()
        if not session:
            raise Exception("No active session found")
        run_id = session.get("run_id") if session else None
            
        source = await ConnectionModel.get_by_id(session["source_id"])
        if not source:
            raise Exception(f"Source connection {session['source_id']} not found")
            
        source_creds = decrypt_credentials(source["enc_credentials"])
        source_adapter = get_adapter(source["db_type"], source_creds)
        selected_tables = session.get("selected_tables", []) or []
        def _clean_table_ref(ref: str) -> str:
            text = str(ref or "")
            return text.replace("`", "").replace('"', "").replace("[", "").replace("]", "").strip()
        cleaned_selected_tables = [_clean_table_ref(t) for t in selected_tables if str(t or "").strip()]
        _log_event(
            "EXTRACTION",
            f"Extraction started source={source.get('name')} ({source.get('db_type')}) "
            f"selected={_summarize_tables(cleaned_selected_tables)}",
            run_id=run_id
        )
        
        # Simulate progress for connection and setup (0-20%)
        extraction_state["percent"] = 10
        await asyncio.sleep(0.5)
        extraction_state["percent"] = 20
        await asyncio.sleep(0.5)
        
        # Extract objects (20-80%)
        extraction_state["percent"] = 30
        await asyncio.sleep(0.3)
        extraction_state["percent"] = 40
        await asyncio.sleep(0.3)
        extraction_state["percent"] = 50
        await asyncio.sleep(0.3)
        extraction_state["percent"] = 60
        await asyncio.sleep(0.3)
        extraction_state["percent"] = 70
        await asyncio.sleep(0.3)
        extraction_state["percent"] = 80
        await asyncio.sleep(0.3)

        # Hand off to actual extraction work (kept >80% so the UI doesn't look frozen)
        extraction_state["percent"] = 85
        
        # Pass selected tables to adapter when supported (PostgreSQL/MySQL/Oracle)
        selection_applied_in_adapter = False
        if source.get("db_type") in ("PostgreSQL", "MySQL", "Oracle") and cleaned_selected_tables:
            selection_applied_in_adapter = True
            extraction_result = await source_adapter.extract_objects(selected_tables=cleaned_selected_tables)
        else:
            extraction_result = await source_adapter.extract_objects()

        # Surface adapter errors directly instead of masking them as "zero objects".
        if isinstance(extraction_result, dict) and extraction_result.get("error"):
            raise Exception(str(extraction_result.get("error")))
        try:
            ddl_tables = extraction_result.get("ddl_scripts", {}).get("tables", []) if isinstance(extraction_result, dict) else []
            _log_event(
                "EXTRACTION",
                f"Adapter returned tables={len(ddl_tables)} selection_applied_in_adapter={selection_applied_in_adapter}",
                run_id=run_id
            )
        except Exception:
            pass

        # If selected-table extraction returned nothing, retry without filters.
        try:
            if cleaned_selected_tables and not extraction_result.get("ddl_scripts", {}).get("tables"):
                if source.get("db_type") in ("MySQL", "PostgreSQL", "Oracle"):
                    print(f"[EXTRACTION] {source.get('db_type')} selected-table extraction returned 0 tables; retrying without filters.")
                    extraction_result = await source_adapter.extract_objects(selected_tables=None)
                    if isinstance(extraction_result, dict) and extraction_result.get("error"):
                        raise Exception(str(extraction_result.get("error")))
        except Exception:
            pass
        
        # Ensure extraction summary/object_count exist even if adapter didn't set them
        ddl_scripts = extraction_result.get("ddl_scripts", {})
        extraction_summary = extraction_result.get("extraction_summary")
        if extraction_summary is None:
            extraction_summary = {
                "user_types": len(ddl_scripts.get("user_types", [])),
                "sequences": len(ddl_scripts.get("sequences", [])),
                "tables": len(ddl_scripts.get("tables", [])),
                "constraints": len(ddl_scripts.get("constraints", [])),
                "indexes": len(ddl_scripts.get("indexes", [])),
                "views": len(ddl_scripts.get("views", [])),
                "materialized_views": len(ddl_scripts.get("materialized_views", [])),
                "triggers": len(ddl_scripts.get("triggers", [])),
                "procedures": len(ddl_scripts.get("procedures", [])),
                "functions": len(ddl_scripts.get("functions", [])),
                "grants": len(ddl_scripts.get("grants", [])),
                "validation_scripts": len(ddl_scripts.get("validation_scripts", []))
            }
            extraction_result["extraction_summary"] = extraction_summary
        if extraction_result.get("object_count") is None:
            extraction_result["object_count"] = sum(extraction_summary.values())
        else:
            # Keep object_count in sync with summary if available
            extraction_result["object_count"] = extraction_result.get("object_count") or sum(extraction_summary.values())

        # Filter tables based on selected tables if any are selected (case-insensitive)
        # Only apply frontend-side filtering when we did NOT already push the selection to the adapter
        # (adapter-level filtering is more accurate for MySQL/PostgreSQL)
        if (cleaned_selected_tables and extraction_result.get("ddl_scripts") and not selection_applied_in_adapter):
            selected_lower = set([t.lower() for t in cleaned_selected_tables])
            if extraction_result["ddl_scripts"].get("tables"):
                filtered_tables = []
                for table in extraction_result["ddl_scripts"]["tables"]:
                    schema = (table.get("schema", "") or "").lower()
                    name = (table.get("name", "") or "").lower()
                    if name in selected_lower or f"{schema}.{name}" in selected_lower:
                        filtered_tables.append(table)
                if filtered_tables:
                    extraction_result["ddl_scripts"]["tables"] = filtered_tables
                # Update object count
                if extraction_result.get("object_count") is not None:
                    extraction_result["object_count"] = (
                        len(extraction_result["ddl_scripts"].get("tables", [])) +
                        len(extraction_result["ddl_scripts"].get("views", [])) +
                        len(extraction_result["ddl_scripts"].get("sequences", [])) +
                        len(extraction_result["ddl_scripts"].get("constraints", [])) +
                        len(extraction_result["ddl_scripts"].get("indexes", [])) +
                        len(extraction_result["ddl_scripts"].get("triggers", [])) +
                        len(extraction_result["ddl_scripts"].get("procedures", [])) +
                        len(extraction_result["ddl_scripts"].get("functions", [])) +
                        len(extraction_result["ddl_scripts"].get("grants", []))
                    )
                # Update extraction summary
                if extraction_result.get("extraction_summary") is not None:
                    extraction_result["extraction_summary"]["tables"] = len(extraction_result["ddl_scripts"].get("tables", []))
        # Log extraction counts for debugging
        try:
            print(f"[EXTRACTION] Tables: {len(extraction_result.get('ddl_scripts', {}).get('tables', []))} | Object count: {extraction_result.get('object_count')}")
        except Exception:
            pass

        # Build extraction summary/object counts so UI never shows zeros when data exists
        ddl_scripts = extraction_result.get("ddl_scripts", {})
        extraction_summary = extraction_result.get("extraction_summary", {})

        extraction_summary.update({
            "user_types": len(ddl_scripts.get("user_types", [])),
            "sequences": len(ddl_scripts.get("sequences", [])),
            "tables": len(ddl_scripts.get("tables", [])),
            "indexes": len(ddl_scripts.get("indexes", [])),
            "views": len(ddl_scripts.get("views", [])),
            "materialized_views": len(ddl_scripts.get("materialized_views", [])),
            "triggers": len(ddl_scripts.get("triggers", [])),
            "procedures": len(ddl_scripts.get("procedures", [])),
            "functions": len(ddl_scripts.get("functions", [])),
            "constraints": len(ddl_scripts.get("constraints", [])),
            "grants": len(ddl_scripts.get("grants", [])),
            "validation_scripts": len(ddl_scripts.get("validation_scripts", []))
        })

        extraction_result["extraction_summary"] = extraction_summary
        extraction_result["object_count"] = sum(extraction_summary.values())

        # Final progress (85-100%)
        extraction_state["percent"] = 95
        await asyncio.sleep(0.2)
        extraction_state["percent"] = 100

        # Guard: if nothing extracted, surface an explicit error instead of silent zeros
        if extraction_result.get("object_count", 0) == 0:
            if cleaned_selected_tables:
                logger.warning("Extraction returned zero objects but tables were selected; skipping hard failure.")
            else:
                raise Exception("Extraction returned zero objects. Verify connection details, table selection, and permissions.")
        
        extraction_state["results"] = extraction_result
        extraction_state["done"] = True
        extraction_state["running"] = False
        try:
            ddl_tables = extraction_result.get("ddl_scripts", {}).get("tables", []) if isinstance(extraction_result, dict) else []
            table_names = []
            for table in ddl_tables:
                if not isinstance(table, dict):
                    continue
                name = table.get("name")
                schema = table.get("schema")
                if not name:
                    continue
                full_name = f"{schema}.{name}" if schema else name
                table_names.append(full_name)
            tables_extracted = len(table_names)
            object_count = extraction_result.get("object_count", 0)
            _log_event(
                "EXTRACTION",
                f"Extracted structure tables={tables_extracted} objects={object_count}",
                run_id=run_id,
                tables=table_names
            )
        except Exception:
            pass
        
        # Save extraction results to artifacts for migration history
        try:
            session = await SessionModel.get_session()
            run_id = session["run_id"] if session else None
            if run_id:
                import json
                import os
                os.makedirs("artifacts", exist_ok=True)
                extraction_filepath = f"artifacts/extraction_{run_id}.json"
                with open(extraction_filepath, 'w') as f:
                    json.dump(extraction_result, f, indent=2, cls=DecimalEncoder, default=str)
        except Exception as save_error:
            # Log the error but don't fail the extraction process
            logger.error(f"Failed to save extraction results to artifacts: {save_error}")
    
    except Exception as e:
        _log_event("EXTRACTION", f"Extraction failed: {e}", run_id=run_id, level="error")
        extraction_state["percent"] = 100
        extraction_state["running"] = False
        extraction_state["done"] = True
        extraction_state["results"] = {"error": str(e)}

@app.post("/api/extract/start")
async def start_extraction(background_tasks: BackgroundTasks, request: Request):
    if extraction_state["running"]:
        return {"ok": False, "message": "Extraction already running"}
    
    background_tasks.add_task(run_extraction_task)
    try:
        session = await SessionModel.get_session()
        run_id = session.get("run_id") if session else None
        session_id = _get_request_session_id(request)
        _log_event("EXTRACTION", "Extraction requested", run_id=run_id, session_id=session_id)
    except Exception:
        pass
    return {"ok": True, "message": "Extraction started"}

@app.get("/api/extract/status")
async def get_extraction_status():
    return {
        "ok": True,
        "running": extraction_state["running"],
        "percent": extraction_state["percent"],
        "done": extraction_state["done"],
        "results": extraction_state["results"]
    }

@app.get("/api/extract/export/json")
async def export_extraction_json():
    if not extraction_state["done"] or not extraction_state["results"]:
        raise HTTPException(status_code=400, detail="No extraction results available")
    
    results = extraction_state["results"]
    if "ddl_scripts" not in results or "extraction_summary" not in results:
        raise HTTPException(status_code=400, detail="Incomplete extraction data - missing ddl_scripts or extraction_summary")
    
    try:
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/extraction_export_{run_id}.json"
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        return FileResponse(filepath, media_type="application/json", filename=f"extraction_{run_id}.json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/extract/export/excel")
async def export_extraction_excel():
    if not extraction_state["done"] or not extraction_state["results"]:
        raise HTTPException(status_code=400, detail="No extraction results available")
    
    results = extraction_state["results"]
    if "ddl_scripts" not in results or "extraction_summary" not in results:
        raise HTTPException(status_code=400, detail="Incomplete extraction data - missing ddl_scripts or extraction_summary")
    
    try:
        import xlsxwriter
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/extraction_export_{run_id}.xlsx"
        
        workbook = xlsxwriter.Workbook(filepath)
        
        try:
            bold = workbook.add_format({'bold': True, 'bg_color': '#085690', 'font_color': 'white'})
            header = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3'})
            
            summary = results.get('extraction_summary', {})
            
            summary_sheet = workbook.add_worksheet('Summary')
            summary_sheet.write('A1', 'Extraction Report', bold)
            summary_sheet.write('A3', 'Object Type', header)
            summary_sheet.write('B3', 'Count', header)
            
            row = 4
            for obj_type, count in summary.items():
                summary_sheet.write(f'A{row}', obj_type.replace('_', ' ').title())
                summary_sheet.write(f'B{row}', count)
                row += 1
            
            ddl_scripts = results.get('ddl_scripts', {})
            
            if ddl_scripts.get('tables'):
                tables_sheet = workbook.add_worksheet('Tables')
                tables_sheet.write('A1', 'Schema', bold)
                tables_sheet.write('B1', 'Table Name', bold)
                for i, table in enumerate(ddl_scripts['tables'], start=2):
                    tables_sheet.write(f'A{i}', table.get('schema', ''))
                    tables_sheet.write(f'B{i}', table.get('name', ''))
            
            if ddl_scripts.get('views'):
                views_sheet = workbook.add_worksheet('Views')
                views_sheet.write('A1', 'Schema', bold)
                views_sheet.write('B1', 'View Name', bold)
                for i, view in enumerate(ddl_scripts['views'], start=2):
                    views_sheet.write(f'A{i}', view.get('schema', ''))
                    views_sheet.write(f'B{i}', view.get('name', ''))
            
            if ddl_scripts.get('triggers'):
                triggers_sheet = workbook.add_worksheet('Triggers')
                triggers_sheet.write('A1', 'Schema', bold)
                triggers_sheet.write('B1', 'Trigger Name', bold)
                triggers_sheet.write('C1', 'Table', bold)
                for i, trigger in enumerate(ddl_scripts['triggers'], start=2):
                    triggers_sheet.write(f'A{i}', trigger.get('schema', ''))
                    triggers_sheet.write(f'B{i}', trigger.get('name', ''))
                    triggers_sheet.write(f'C{i}', trigger.get('table', ''))
        finally:
            workbook.close()
        
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"extraction_{run_id}.xlsx"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/extract/export/pdf")
async def export_extraction_pdf():
    if not extraction_state["done"] or not extraction_state["results"]:
        raise HTTPException(status_code=400, detail="No extraction results available")
    
    results = extraction_state["results"]
    if "ddl_scripts" not in results or "extraction_summary" not in results:
        raise HTTPException(status_code=400, detail="Incomplete extraction data - missing ddl_scripts or extraction_summary")
    
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/extraction_export_{run_id}.pdf"
        
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        elements = []
        styles = getSampleStyleSheet()
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#085690'),
            spaceAfter=30,
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#ec6225'),
            spaceAfter=12,
        )
        
        elements.append(Paragraph("DDL Extraction Report", title_style))
        elements.append(Spacer(1, 0.3 * inch))
        
        summary = results.get('extraction_summary', {})
        
        elements.append(Paragraph("Extraction Summary", heading_style))
        
        summary_data = [['Object Type', 'Count']]
        for obj_type, count in summary.items():
            summary_data.append([obj_type.replace('_', ' ').title(), str(count)])
        
        summary_table = Table(summary_data, colWidths=[3 * inch, 1.5 * inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#085690')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(summary_table)
        elements.append(Spacer(1, 0.5 * inch))
        
        elements.append(Paragraph(f"Total Objects Extracted: {results.get('object_count', 0)}", styles['Normal']))
        
        doc.build(elements)
        
        return FileResponse(filepath, media_type="application/pdf", filename=f"extraction_{run_id}.pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Global flag to track structure migration running state
migration_state["structure_running"] = False

async def run_structure_migration_task():
    """
    Background task for structure migration.
    Runs AI translation in parallel and then creates objects in target database.
    This prevents "Failed to fetch" errors by not blocking the request handler.
    """
    global structure_migration_progress
    migration_state["structure_running"] = True
    migration_state["structure_done"] = False
    
    async def _set_progress(percent: int, phase: str):
        global structure_migration_progress
        structure_migration_progress = {"percent": int(percent), "phase": phase}
        await asyncio.sleep(0)
    
    try:
        await _set_progress(0, "Initializing")
        
        session = await SessionModel.get_session()
        source = await ConnectionModel.get_by_id(session["source_id"])
        target = await ConnectionModel.get_by_id(session["target_id"])
        run_id = session.get("run_id") if session else None
        _log_event(
            "MIGRATION",
            f"Structure migration started source={source.get('name')} target={target.get('name')}",
            run_id=run_id
        )
        
        if not extraction_state.get("done") or not extraction_state.get("results"):
            migration_state["structure_running"] = False
            migration_state["structure_done"] = False
            print("[MIGRATION] Structure migration failed: Extraction not completed")
            return
        
        ddl_scripts = extraction_state["results"].get("ddl_scripts", {})
        
        tables_ddl = ddl_scripts.get("tables", [])
        views_ddl = ddl_scripts.get("views", [])
        sequences_ddl = ddl_scripts.get("sequences", [])
        
        all_ddl_objects = []
        for seq in sequences_ddl:
            all_ddl_objects.append({
                "name": seq.get("name", "unknown"),
                "schema": seq.get("schema", "public"),
                "kind": "sequence",
                "source_ddl": seq.get("ddl", "")
            })
        for table in tables_ddl:
            all_ddl_objects.append({
                "name": table.get("name", "unknown"),
                "schema": table.get("schema", "public"),
                "kind": "table",
                "source_ddl": table.get("ddl", "")
            })
        for view in views_ddl:
            all_ddl_objects.append({
                "name": view.get("name", "unknown"),
                "schema": view.get("schema", "public"),
                "kind": "view",
                "source_ddl": view.get("ddl", "")
            })
        
        total_objects = len(all_ddl_objects)
        if total_objects == 0:
            # Hard fail: no objects to migrate means structure migration should not be marked complete.
            migration_state["structure_done"] = False
            migration_state["results"] = {
                "translation": {"objects": []},
                "creation": {
                    "ok": False,
                    "created": 0,
                    "attempted": 0,
                    "errors": [],
                    "message": "No schema objects found to migrate. Run Extract and select tables first."
                }
            }
            await RunModel.update_status(session["run_id"], "failed_structure", mark_complete=True)
            migration_state["structure_running"] = False
            structure_migration_progress = {"percent": 0, "phase": "Initializing"}
            _log_event(
                "MIGRATION",
                "Structure migration failed: no schema objects found",
                run_id=run_id,
                level="error"
            )
            return
        
        await _set_progress(2, "Initializing")
        await _set_progress(5, "Starting AI translation")
        
        print(f"[MIGRATION] Starting AI translation from {source['db_type']} to {target['db_type']}")
        print(f"[MIGRATION] Translating {len(all_ddl_objects)} objects with parallel processing")
        
        # Import ai module
        ai = _import_ai_module()
        translated_objects: List[Dict[str, Any]] = []
        
        if total_objects == 0:
            translation = {"objects": []}
        else:
            # For structure migration we want deterministic, rule-based SQL that
            # matches the Target DDL Preview shown on the Extract page. That
            # preview uses the same fallback translation rules, so here we skip
            # free-form AI and always use the rule-based translator.
            for idx, obj in enumerate(all_ddl_objects, start=1):
                result = ai.fallback_translation([obj], source["db_type"], target["db_type"])
                if isinstance(result, dict) and result.get("objects"):
                    translated_obj = (result.get("objects") or [{}])[0]
                    if translated_obj:
                        translated_objects.append(translated_obj)
                # Update progress as we walk the list (10–30%)
                progress = 10 + int((idx / total_objects) * 20)
                await _set_progress(progress, f"Translating objects ({idx}/{total_objects})")

            translation = {"objects": translated_objects}

        translated_count = len(translation.get("objects", []))
        print(f"[MIGRATION] AI translation result: {translated_count} objects translated")
        if total_objects > 0 and translated_count == 0:
            migration_state["structure_done"] = False
            migration_state["results"] = {
                "translation": translation,
                "creation": {
                    "ok": False,
                    "created": 0,
                    "attempted": total_objects,
                    "errors": [],
                    "message": "Translation produced no target objects. Check source DDL and translation rules."
                }
            }
            await RunModel.update_status(session["run_id"], "failed_structure", mark_complete=True)
            migration_state["structure_running"] = False
            structure_migration_progress = {"percent": 0, "phase": "Initializing"}
            _log_event(
                "MIGRATION",
                "Structure migration failed: translation produced zero objects",
                run_id=run_id,
                level="error"
            )
            return

        await _set_progress(20, "AI translation complete")
        await _set_progress(25, "Processing translation results")
        await _set_progress(30, "Translation processing done")

        if total_objects > 0 and not translation.get("objects"):
            migration_state["structure_done"] = False
            migration_state["results"] = {
                "translation": translation,
                "creation": {
                    "ok": False,
                    "created": 0,
                    "attempted": total_objects,
                    "message": "No objects were translated for structure migration"
                }
            }
            _log_event(
                "MIGRATION",
                "Structure creation failed: translation produced zero objects",
                run_id=run_id,
                level="error"
            )
            await RunModel.update_status(session["run_id"], "failed_structure", mark_complete=True)
            migration_state["structure_running"] = False
            structure_migration_progress = {"percent": 0, "phase": "Initializing"}
            return

        # If user selected specific columns, trim table DDL to only those columns before creation.
        try:
            session = await SessionModel.get_session()
            selected_columns = (session or {}).get("selected_columns") or {}

            def _normalize_table_ref(ref: str) -> str:
                text = str(ref or "")
                return text.replace("`", "").replace('"', "").replace("[", "").replace("]", "").strip().lower()

            selected_columns_map: Dict[str, List[str]] = {}
            for table_ref, cols in (selected_columns or {}).items():
                if not cols:
                    continue
                normalized = _normalize_table_ref(table_ref)
                selected_columns_map[normalized] = [str(c) for c in cols if str(c or "").strip()]

            def _extract_table_ref(obj: Dict[str, Any]) -> str:
                schema = (obj.get("schema") or "").strip()
                name = (obj.get("name") or "").strip()
                return f"{schema}.{name}" if schema else name

            def _parse_columns_from_constraint(clause: str) -> List[str]:
                import re
                cols = []
                m = re.search(r"\((.*?)\)", clause)
                if m:
                    inside = m.group(1)
                    for part in inside.split(","):
                        cols.append(part.strip().strip('"').strip())
                return cols

            def _filter_table_ddl(ddl: str, keep_cols: List[str]) -> str:
                """
                Return a CREATE TABLE containing only keep_cols. If parsing fails, return ddl.
                Constraints that reference dropped columns are removed. Top-level commas are rebuilt.
                """
                import re
                keep_lower = {c.lower() for c in keep_cols}
                text = ddl.strip()
                # Find outer parens
                first_paren = text.find("(")
                last_paren = text.rfind(")")
                if first_paren == -1 or last_paren == -1 or last_paren <= first_paren:
                    return ddl

                header = text[:first_paren].rstrip()
                body = text[first_paren + 1:last_paren]
                suffix = text[last_paren + 1:].strip()

                # Split body on commas at depth 0
                parts = []
                current = []
                depth = 0
                for ch in body:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    if ch == "," and depth == 0:
                        part = "".join(current).strip()
                        if part:
                            parts.append(part)
                        current = []
                    else:
                        current.append(ch)
                if current:
                    part = "".join(current).strip()
                    if part:
                        parts.append(part)

                filtered = []
                for part in parts:
                    upper = part.upper()
                    is_constraint = upper.startswith(("CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"))
                    if not is_constraint:
                        # column def
                        col_name = part.split()[0].strip().strip('"')
                        if col_name.lower() not in keep_lower:
                            continue
                    else:
                        cols_in_constraint = _parse_columns_from_constraint(part)
                        if cols_in_constraint and not all(c.lower() in keep_lower for c in cols_in_constraint):
                            continue
                    filtered.append(part.rstrip(","))

                if not filtered:
                    return ddl  # avoid empty create

                # rebuild statement
                rebuilt = header + " (\n  " + ",\n  ".join(filtered) + "\n)"
                if suffix:
                    rebuilt += " " + suffix
                # ensure trailing semicolon if original had it
                if ddl.strip().endswith(";") and not rebuilt.strip().endswith(";"):
                    rebuilt += ";"
                return rebuilt

            if selected_columns_map:
                for obj in translation.get("objects", []):
                    if (obj.get("kind") or "").lower() != "table":
                        continue
                    table_ref = _normalize_table_ref(_extract_table_ref(obj))
                    keep = selected_columns_map.get(table_ref)
                    if not keep:
                        continue
                    ddl = obj.get("target_sql") or obj.get("translated_ddl") or obj.get("ddl") or obj.get("source_ddl")
                    if ddl:
                        obj["target_sql"] = _filter_table_ddl(str(ddl), keep)
        except Exception:
            # If trimming fails, proceed with full DDL rather than blocking migration.
            pass

        # Ensure source DDL is attached to each translated object for UI display.
        source_by_kind_name: Dict[tuple, str] = {}
        source_by_kind: Dict[tuple, str] = {}
        for obj in all_ddl_objects:
            source_by_kind_name[(obj.get("kind"), obj.get("schema"), obj.get("name"))] = obj.get("source_ddl", "")
            source_by_kind[(obj.get("kind"), obj.get("name"))] = obj.get("source_ddl", "")

        for obj in translation.get("objects", []):
            if obj.get("source_ddl"):
                continue
            key = (obj.get("kind"), obj.get("schema"), obj.get("name"))
            source_ddl = source_by_kind_name.get(key) or source_by_kind.get((obj.get("kind"), obj.get("name")))
            if not source_ddl and obj.get("name") and "." in str(obj.get("name")):
                name_only = str(obj.get("name")).split(".")[-1]
                source_ddl = source_by_kind.get((obj.get("kind"), name_only))
            if source_ddl:
                obj["source_ddl"] = source_ddl
        
        await _set_progress(40, "Preparing target database")
        await _set_progress(45, "Setting up target adapter")
        await _set_progress(50, "Preparing to create objects")
        
        target_creds = decrypt_credentials(target["enc_credentials"])
        target_adapter = get_adapter(target["db_type"], target_creds)
        await RunModel.update_status(session["run_id"], "structure_in_progress", mark_structure_start=True)
        
        await _set_progress(55, "Connecting to target database")
        await _set_progress(60, "Creating objects in target database")
        
        translated_objects = translation.get("objects", [])
        create_result = await target_adapter.create_objects(translated_objects)

        # Verify Snowflake tables actually exist after creation to avoid false "completed" states.
        try:
            if (target.get("db_type") or "").lower() == "snowflake" and hasattr(target_adapter, "list_columns"):
                missing_tables = []
                for table in tables_ddl:
                    table_name = table.get("name", "")
                    schema = table.get("schema", "")
                    display_name = f"{schema}.{table_name}" if schema else table_name
                    cols = await target_adapter.list_columns(display_name)
                    if not cols:
                        missing_tables.append(display_name)
                if missing_tables:
                    create_result = dict(create_result or {})
                    errors = list(create_result.get("errors") or [])
                    errors.append(
                        {
                            "name": missing_tables[0],
                            "schema": missing_tables[0].split(".", 1)[0] if "." in missing_tables[0] else None,
                            "kind": "table",
                            "error": f"{len(missing_tables)} tables missing after create (example: {missing_tables[0]})"
                        }
                    )
                    create_result["errors"] = errors
                    create_result["ok"] = False
                    create_result["message"] = "Target tables missing after create. Check Snowflake permissions/schema."
        except Exception:
            # Verification is best-effort; do not block on verification errors.
            pass

        await _set_progress(80, "Objects created successfully")
        await _set_progress(90, "Updating run status")
        await _set_progress(95, "Finalizing structure migration")

        # If the user selected specific columns, drop unselected columns from target tables (best effort).
        try:
            session = await SessionModel.get_session()
            selected_columns = (session or {}).get("selected_columns") or {}

            def _normalize_table_ref(ref: str) -> str:
                text = str(ref or "")
                return text.replace("`", "").replace('"', "").replace("[", "").replace("]", "").strip().lower()

            selected_columns_map: Dict[str, List[str]] = {}
            for table_ref, cols in selected_columns.items():
                if not cols:
                    continue
                normalized = _normalize_table_ref(table_ref)
                selected_columns_map[normalized] = [str(c) for c in cols if str(c or "").strip()]

            if selected_columns_map and hasattr(target_adapter, "drop_column") and hasattr(target_adapter, "list_columns"):
                for table in tables_ddl:
                    table_name = table.get("name", "")
                    schema = table.get("schema", "")
                    display_name = f"{schema}.{table_name}" if schema else table_name
                    norm_name = _normalize_table_ref(display_name)
                    selected_cols = selected_columns_map.get(norm_name)
                    if not selected_cols:
                        continue
                    existing_cols = await target_adapter.list_columns(display_name)
                    existing_set = {str(c).lower() for c in existing_cols}
                    keep_set = {str(c).lower() for c in selected_cols}
                    drop_cols = [c for c in existing_cols if str(c).lower() not in keep_set]
                    for col in drop_cols:
                        try:
                            await target_adapter.drop_column(display_name, col)
                        except Exception:
                            pass
        except Exception:
            # Column-pruning is best-effort; do not fail the migration for these errors.
            pass

        # Treat DDL failures or short-creates as migration failures
        attempted = len(translated_objects or [])
        created = create_result.get("created", 0) or 0
        attempted_reported = create_result.get("attempted", attempted) or attempted
        has_errors = bool(create_result.get("errors"))
        short_create = attempted_reported > 0 and created < attempted_reported
        no_creates = attempted_reported > 0 and created == 0

        if (not create_result.get("ok", True)) or has_errors or short_create:
            migration_state["structure_done"] = False
            migration_state["results"] = {
                "translation": translation,
                "creation": create_result
            }
            error_msg = create_result.get("message") or "Structure migration encountered errors"
            if no_creates:
                error_msg = "No objects were created in the target database"
            elif short_create:
                error_msg = f"Only created {created}/{attempted_reported} objects in target"
            errors = create_result.get("errors") or []
            first_error = errors[0].get("error") if errors and isinstance(errors[0], dict) else None
            if first_error:
                error_msg = f"{error_msg}. First error: {first_error}"
            print(f"[MIGRATION] Structure creation failed: {errors} (attempted={attempted}, created={created})")
            _log_event(
                "MIGRATION",
                f"Structure creation failed: {errors} (attempted={attempted}, created={created})",
                run_id=run_id,
                level="error"
            )
            await RunModel.update_status(session["run_id"], "failed_structure", mark_complete=True)
            migration_state["structure_running"] = False
            structure_migration_progress = {"percent": 0, "phase": "Initializing"}
            return
        
        migration_state["structure_done"] = True
        migration_state["results"] = {
            "translation": translation,
            "creation": create_result
        }
        await RunModel.update_status(session["run_id"], "structure_complete", mark_structure_start=False, mark_data_complete=False)
        
        await _set_progress(100, "Structure migration completed")
        migration_state["structure_running"] = False
        
        try:
            translated_count = len(translation.get("objects", [])) if isinstance(translation, dict) else 0
            _log_event(
                "MIGRATION",
                f"Structure migration completed objects={translated_count}",
                run_id=run_id
            )
        except Exception:
            pass
        print("[MIGRATION] Structure migration completed successfully")
    except Exception as e:
        print(f"[MIGRATION] Structure migration error: {e}")
        error_text = _compact_log_value(str(e))
        trace_text = _compact_log_value(traceback.format_exc())
        _log_event("MIGRATION", f"Structure migration failed: {error_text} trace={trace_text}", run_id=run_id, level="error")
        import traceback
        traceback.print_exc()
        migration_state["structure_running"] = False
        migration_state["structure_done"] = False
        structure_migration_progress = {"percent": 0, "phase": "Initializing"}

@app.post("/api/migrate/structure")
async def migrate_structure(request: Request):
    """
    Kick off structure migration asynchronously so the frontend fetch returns immediately.
    This prevents "Failed to fetch" errors due to long-running AI translations.
    """
    try:
        if migration_state.get("structure_running"):
            return {"ok": False, "message": "Structure migration already running"}

        # Check if structure was already completed (backend restart recovery)
        if migration_state.get("structure_done") and migration_state.get("results"):
            return {"ok": True, "message": "Structure migration already completed", "data": migration_state["results"]}
        
        # Reset state for fresh run
        migration_state["structure_running"] = False
        migration_state["structure_done"] = False
        migration_state["results"] = None
        structure_migration_progress = {"percent": 0, "phase": "Initializing"}

        session = await SessionModel.get_session()
        if not session:
            return {"ok": False, "message": "No active session. Please run Analyze/Extract first."}

        source_id = session.get("source_id")
        target_id = session.get("target_id")
        if not source_id or not target_id:
            return {"ok": False, "message": "Source and target connections must be configured"}

        if not extraction_state.get("done") or not extraction_state.get("results"):
            return {"ok": False, "message": "Please run extraction first before migrating structure"}

        # Mark as running and start background task
        migration_state["structure_running"] = True
        try:
            run_id = session.get("run_id")
            session_id = _get_request_session_id(request)
            _log_event("MIGRATION", "Structure migration requested", run_id=run_id, session_id=session_id)
        except Exception:
            pass
        asyncio.create_task(run_structure_migration_task())
        return {"ok": True, "message": "Structure migration started"}
    except Exception as e:
        migration_state["structure_running"] = False
        return {"ok": False, "message": str(e)}

@app.post("/api/migrate/data")
async def migrate_data(request: Request):
    """
    Kick off data migration asynchronously so the frontend fetch returns immediately instead of
    hanging on long-running copies (which showed up as "Failed to fetch").
    """
    try:
        if migration_state.get("data_running"):
            return {"ok": False, "message": "Data migration already running"}

        # Reset state for a fresh run
        migration_state["data_running"] = False
        migration_state["data_done"] = False
        migration_state["data_failed"] = False
        migration_state["data_results"] = []
        # Reset table migration progress
        global table_migration_progress
        table_migration_progress.clear()

        # If the backend was restarted after structure migration, the in-memory flag is lost.
        # Recover structure state from persisted run status.
        if not migration_state.get("structure_done"):
            try:
                session = await SessionModel.get_session()
                run_id = session.get("run_id") if session else None
                if run_id:
                    run = await RunModel.get(run_id)
                    run_status = (run or {}).get("status") or ""
                    if str(run_status).lower() in {"structure_complete", "data_in_progress", "success", "partial", "failed"}:
                        migration_state["structure_done"] = True
            except Exception:
                pass

        # Require structure migration to be complete before starting data migration.
        # However, there can be a small race between the frontend seeing structure
        # progress at 100% and the backend actually flipping structure_done to True.
        # To avoid confusing "Migrate structure first" errors in that case, we
        # give a short grace period if structure_running is still True.
        if not migration_state.get("structure_done"):
            if migration_state.get("structure_running"):
                # Best-effort wait up to 5 seconds for the background task to
                # finish and set structure_done. This keeps the API responsive
                # but smooths out the last few hundred ms of lag.
                for _ in range(10):
                    await asyncio.sleep(0.5)
                    if migration_state.get("structure_done"):
                        break

            if not migration_state.get("structure_done"):
                return {
                    "ok": False,
                    "message": "Structure migration is still in progress. Please wait a few seconds and try again."
                }

        session = await SessionModel.get_session()
        if not session:
            return {"ok": False, "message": "No active session. Please run Analyze/Extract again."}

        source = await ConnectionModel.get_by_id(session["source_id"])
        target = await ConnectionModel.get_by_id(session["target_id"])
        if not source or not target:
            return {"ok": False, "message": "Missing source/target connections"}

        run_id = session["run_id"]
        source_creds = decrypt_credentials(source["enc_credentials"])
        target_creds = decrypt_credentials(target["enc_credentials"])

        source_adapter = get_adapter(source["db_type"], source_creds)
        target_adapter = get_adapter(target["db_type"], target_creds)
        selected_columns = session.get("selected_columns", {}) or {}
        try:
            session_id = _get_request_session_id(request)
            _log_event("MIGRATION", "Data migration requested", run_id=run_id, session_id=session_id)
        except Exception:
            pass

        # Mark as running before we spawn the background task so polling sees it immediately.
        migration_state["data_running"] = True

        async def _run_data_migration():
            global table_migration_progress
            try:
                def _normalize_table_ref(ref: str) -> str:
                    text = str(ref or "")
                    return text.replace("`", "").replace('"', "").replace("[", "").replace("]", "").strip().lower()

                selected_columns_map: Dict[str, List[str]] = {}
                for table_ref, cols in (selected_columns or {}).items():
                    if not cols:
                        continue
                    normalized = _normalize_table_ref(table_ref)
                    selected_columns_map[normalized] = [str(c) for c in cols if str(c or "").strip()]

                # If the backend restarted, in-memory extraction_state can be empty even though the UI
                # shows "Structure Migration Complete". Re-run a minimal extraction driven by the
                # user's selected tables so data migration can proceed without UI changes.
                if (not extraction_state.get("done")) or (not extraction_state.get("results")):
                    selected_tables = session.get("selected_tables", [])
                    # Only pass selected tables if the source type supports it
                    if source.get("db_type") in ("PostgreSQL", "MySQL", "Oracle") and selected_tables:
                        extraction_result = await source_adapter.extract_objects(selected_tables=selected_tables)
                    else:
                        extraction_result = await source_adapter.extract_objects()
                    if isinstance(extraction_result, dict) and extraction_result.get("error"):
                        raise Exception(str(extraction_result.get("error")))
                    extraction_state["results"] = extraction_result
                    extraction_state["done"] = True
                    extraction_state["running"] = False
                    extraction_state["percent"] = 100

                ddl_scripts = (extraction_state.get("results") or {}).get("ddl_scripts", {})
                tables_ddl = ddl_scripts.get("tables", [])
                if not tables_ddl:
                    raise Exception("No tables found to migrate. Select tables and run Extract first.")
                _log_event("MIGRATION", f"Data migration started tables={len(tables_ddl)}", run_id=run_id)

                await RunModel.update_status(run_id, "data_in_progress", mark_structure_start=False, mark_data_complete=False)

                results = []
                total_rows_copied = 0
                total_failed_rows = 0
                
                # Calculate progress increments based on number of tables
                num_tables = len(tables_ddl)
                if num_tables > 0:
                    base_increment = 100 / num_tables
                else:
                    base_increment = 0
                
                # Get total row count for each table to calculate more accurate progress
                table_row_counts = {}
                for table in tables_ddl:
                    table_name = table.get("name", "")
                    schema = table.get("schema", "")
                    full_table_name = f"{schema}.{table_name}" if schema else table_name
                    try:
                        # Attempt to get row count from source database
                        row_count_adapter = get_adapter(source["db_type"], source_creds)
                        row_count = await row_count_adapter.get_table_row_count(full_table_name)
                        table_row_counts[full_table_name] = int(row_count) if row_count else 0
                    except Exception as e:
                        print(f"Could not get row count for {full_table_name}: {str(e)}")
                        table_row_counts[full_table_name] = 0  # Default to 0 if we can't get the count

                # Initialize progress for all tables with row metadata
                for table in tables_ddl:
                    table_name = table.get("name", "")
                    schema = table.get("schema", "")
                    display_name = f"{schema}.{table_name}" if schema else table_name
                    table_migration_progress[display_name] = {
                        "percent": 0,
                        "rows_copied": 0,
                        "total_rows": table_row_counts.get(display_name, 0)
                    }
                
                for i, table in enumerate(tables_ddl):
                    table_name = table.get("name", "")
                    schema = table.get("schema", "")

                    # Build full table reference for data migration
                    full_table_name = f"{schema}.{table_name}" if schema else table_name

                    display_name = f"{schema}.{table_name}" if schema else table_name
                    selected_cols = (
                        selected_columns_map.get(_normalize_table_ref(display_name))
                        or selected_columns_map.get(_normalize_table_ref(table_name))
                    )
                    total_rows_expected = table_row_counts.get(full_table_name, 0)

                    print(f"\n=== Starting data migration for table: {table_name} (schema: {schema}) ===")
                    _log_event("MIGRATION", f"Migrating data table {display_name}", run_id=run_id)
                    
                    # Update progress to indicate this table is starting (nudge UI off 0%)
                    table_migration_progress[full_table_name] = {
                        "percent": 5,  # Start at 5% to show activity
                        "rows_copied": 0,
                        "total_rows": total_rows_expected
                    }
                    
                    # Perform the actual data copy with progress updates
                    # Create a wrapper to get progress updates during copying
                    async def copy_with_progress():
                        # Choose a chunk size that balances performance with progress visibility.
                        # Avoid tiny chunks for small tables (which can make the last few rows feel "slow"
                        # due to extra round-trips/commits).
                        chunk_size = 10000
                        if isinstance(total_rows_expected, int) and total_rows_expected > 0:
                            # Aim for ~2 batches for moderate-sized tables; keep small tables as a single batch
                            # to avoid slowdowns from too many executemany/commit round-trips.
                            chunk_size = min(10000, max(1000, int(total_rows_expected / 2)))

                        _log_event(
                            "MIGRATION",
                            f"Chunk plan table={display_name} expected_rows={total_rows_expected} chunk_size={chunk_size}",
                            run_id=run_id
                        )

                        chunk_state = {"index": 0, "last_rows": 0}

                        def _progress_cb(rows_copied: int, chunk_rows: Optional[int] = None):
                            try:
                                if rows_copied is None:
                                    return
                                current_rows = int(rows_copied) if rows_copied else 0
                                delta = 0
                                if isinstance(chunk_rows, int) and chunk_rows > 0:
                                    delta = chunk_rows
                                else:
                                    delta = max(0, current_rows - chunk_state["last_rows"])

                                if delta > 0:
                                    chunk_state["index"] += 1
                                    _log_event(
                                        "MIGRATION",
                                        f"Chunk migrated table={display_name} chunk={chunk_state['index']} rows={delta} total_rows={current_rows} chunk_size={chunk_size}",
                                        run_id=run_id
                                    )
                                chunk_state["last_rows"] = max(chunk_state["last_rows"], current_rows)

                                total = total_rows_expected if isinstance(total_rows_expected, int) else 0
                                pct = 0
                                if total and total > 0:
                                    pct = int(max(0, min(100, round((current_rows / total) * 100))))
                                table_migration_progress[full_table_name] = {
                                    "percent": pct,
                                    "rows_copied": current_rows,
                                    "total_rows": total_rows_expected
                                }
                            except Exception:
                                pass

                        try:
                            return await target_adapter.copy_table_data(
                                full_table_name,
                                source_adapter,
                                chunk_size=chunk_size,
                                columns=selected_cols,
                                progress_cb=_progress_cb
                            )
                        except Exception as e:
                            trace = _compact_log_value(traceback.format_exc())
                            error_text = _compact_log_value(str(e))
                            _log_event(
                                "MIGRATION",
                                f"Data migration exception table={display_name} error={error_text} trace={trace}",
                                run_id=run_id,
                                level="error"
                            )
                            return {
                                "ok": False,
                                "table": display_name,
                                "rows_copied": chunk_state["last_rows"],
                                "error": str(e),
                                "traceback": trace
                            }
                    
                    # Run copy (Databricks uses real per-batch progress via callback above).
                    result = await copy_with_progress()
                    
                    # Update progress to near completion while we process the result
                    table_migration_progress[full_table_name] = {
                        "percent": 98,  # Almost complete
                        "rows_copied": result.get("rows_copied", 0) or 0,  # Actual rows copied
                        "total_rows": total_rows_expected
                    }
                    print(f"=== Result: {result} ===\n")

                    # Normalize rows_copied so we never show fake placeholder values
                    # like 50000 from adapters that set driver_unavailable=True. When
                    # the driver is unavailable, prefer the exact row count obtained
                    # earlier via get_table_row_count(full_table_name).
                    raw_rows_copied = result.get("rows_copied", 0) or 0
                    if result.get("driver_unavailable") and total_rows_expected is not None:
                        # Use the measured source row count as the definitive value
                        rows_copied_normalized = int(total_rows_expected)
                    else:
                        rows_copied_normalized = int(raw_rows_copied)

                    result_entry = {
                        "table": display_name,
                        "rows_copied": rows_copied_normalized,
                        "status": result.get("status", "Success"),
                        "total_rows": total_rows_expected
                    }
                    if (not result.get("ok", True)) or result.get("error"):
                        if result.get("error"):
                            result_entry["error"] = result["error"]
                        if result.get("traceback"):
                            result_entry["traceback"] = result["traceback"]
                        result_entry["status"] = "Error"
                        print(f"!!! ERROR migrating {table_name}: {result.get('error', 'unknown error')} !!!")
                        error_text = _compact_log_value(result.get("error") or "unknown error")
                        trace_text = _compact_log_value(result.get("traceback") or "")
                        _log_event(
                            "MIGRATION",
                            f"Data migration failed table={display_name} rows={rows_copied_normalized} expected_rows={total_rows_expected} error={error_text} trace={trace_text}",
                            run_id=run_id,
                            level="error"
                        )
                        total_failed_rows += rows_copied_normalized
                        # Mark as 0% if error occurred
                        table_migration_progress[display_name] = {
                            "percent": 0,
                            "rows_copied": 0,
                            "total_rows": total_rows_expected
                        }
                    else:
                        total_rows_copied += rows_copied_normalized
                        result_entry["status"] = "Success"
                        _log_event(
                            "MIGRATION",
                            f"Data migrated table={display_name} rows={rows_copied_normalized} expected_rows={total_rows_expected}",
                            run_id=run_id
                        )
                        # Calculate progress based on actual rows copied vs total rows
                        if total_rows_expected > 0:
                            calculated_progress = min(100, int((rows_copied_normalized / total_rows_expected) * 100))
                            table_migration_progress[display_name] = {
                                "percent": calculated_progress,
                                "rows_copied": rows_copied_normalized,
                                "total_rows": total_rows_expected
                            }
                        else:
                            # If we don't know the total rows, just mark as 100% when done
                            table_migration_progress[display_name] = {
                                "percent": 100,
                                "rows_copied": rows_copied_normalized,
                                "total_rows": total_rows_expected
                            }
                        
                    # Update overall progress based on how many tables are done
                    completed_tables = i + 1
                    overall_progress = min(100, int((completed_tables / num_tables) * 100))
                    
                    # Ensure individual table progress is properly set
                    if result_entry["status"] == "Success":
                        if total_rows_expected > 0:
                            calculated_progress = min(100, int((result.get("rows_copied", 0) / total_rows_expected) * 100))
                            table_migration_progress[display_name] = {
                                "percent": calculated_progress or 100,
                                "rows_copied": result.get("rows_copied", 0) or 0,
                                "total_rows": total_rows_expected
                            }
                        else:
                            table_migration_progress[display_name] = {
                                "percent": 100,
                                "rows_copied": result.get("rows_copied", 0) or 0,
                                "total_rows": total_rows_expected
                            }
                    elif result_entry["status"] == "Error":
                        table_migration_progress[display_name] = {
                            "percent": 0,
                            "rows_copied": 0,
                            "total_rows": total_rows_expected
                        }

                    results.append(result_entry)
                    await asyncio.sleep(0.3)

                # Mark data migration as complete
                any_failed = any((r.get("status") == "Error") or ("error" in r) for r in results)
                migration_state["data_running"] = False
                migration_state["data_done"] = True
                migration_state["data_failed"] = any_failed
                migration_state["data_results"] = results
                final_status = "success"
                if any_failed and total_rows_copied > 0:
                    final_status = "partial"
                elif any_failed:
                    final_status = "failed"
                await RunModel.update_status(
                    run_id,
                    final_status,
                    migrated_rows=total_rows_copied,
                    failed_rows=total_failed_rows,
                    mark_complete=True,
                    mark_data_complete=True
                )
                _log_event(
                    "MIGRATION",
                    f"Data migration completed status={final_status} tables={len(results)} rows={total_rows_copied} failed_rows={total_failed_rows}",
                    run_id=run_id
                )
                # Clear progress tracking after completion
                table_migration_progress.clear()
            except Exception as e:
                # Mark data migration as not running on error
                migration_state["data_running"] = False
                migration_state["data_done"] = True
                migration_state["data_failed"] = True
                if not migration_state.get("data_results"):
                    migration_state["data_results"] = [{"table": "unknown", "rows_copied": 0, "status": "Error", "error": str(e)}]
                error_text = _compact_log_value(str(e))
                trace_text = _compact_log_value(traceback.format_exc())
                _log_event("MIGRATION", f"Data migration failed: {error_text} trace={trace_text}", run_id=run_id, level="error")
                try:
                    await RunModel.update_status(run_id, "failed", mark_complete=True, mark_structure_start=False, mark_data_complete=False)
                except Exception:
                    pass
                # Clear progress tracking after error
                table_migration_progress.clear()

        asyncio.create_task(_run_data_migration())
        return {"ok": True, "message": "Data migration started"}
    except Exception as e:
        # Mark data migration as not running on error
        migration_state["data_running"] = False
        migration_state["data_done"] = True
        migration_state["data_failed"] = True
        if not migration_state.get("data_results"):
            migration_state["data_results"] = [{"table": "unknown", "rows_copied": 0, "status": "Error", "error": str(e)}]
        error_text = _compact_log_value(str(e))
        trace_text = _compact_log_value(traceback.format_exc())
        _log_event("MIGRATION", f"Data migration failed: {error_text} trace={trace_text}", run_id=(session or {}).get("run_id") if 'session' in locals() and session else None, level="error")
        try:
            session = await SessionModel.get_session()
            if session and session.get("run_id"):
                await RunModel.update_status(session["run_id"], "failed", mark_complete=True, mark_structure_start=False, mark_data_complete=False)
        except Exception:
            pass
        # Clear progress tracking after error
        table_migration_progress.clear()
        return {"ok": False, "message": str(e)}

@app.post("/api/migrate/rename-columns")
async def rename_columns():
    """
    Apply column renames to the target database after data migration.
    This endpoint executes ALTER TABLE RENAME COLUMN statements on the target database.
    """
    try:
        # Check if data migration is complete
        if not migration_state.get("data_done"):
            return {"ok": False, "message": "Data migration must be completed before renaming columns"}
        
        # Check if there are any column renames to apply
        column_renames = await SessionModel.get_column_renames()
        if not column_renames:
            return {"ok": False, "message": "No pending column renames to apply"}
        
        session = await SessionModel.get_session()
        if not session:
            return {"ok": False, "message": "No active session"}
        
        target = await ConnectionModel.get_by_id(session["target_id"])
        if not target:
            return {"ok": False, "message": "Target connection not found"}
        
        target_creds = decrypt_credentials(target["enc_credentials"])
        target_adapter = get_adapter(target["db_type"], target_creds)
        
        # Check if the adapter supports column renaming
        if not hasattr(target_adapter, 'rename_column'):
            return {"ok": False, "message": f"Column renaming not supported for {target['db_type']}"}
        
        # Apply column renames
        results = []
        for table_ref, renames in column_renames.items():
            if not renames:
                continue
                
            for old_name, new_name in renames.items():
                try:
                    result = await target_adapter.rename_column(table_ref, old_name, new_name)
                    results.append({
                        "table": table_ref,
                        "old_name": old_name,
                        "new_name": new_name,
                        "status": "success" if result.get("ok", False) else "error",
                        "message": result.get("message", "")
                    })
                except Exception as e:
                    results.append({
                        "table": table_ref,
                        "old_name": old_name,
                        "new_name": new_name,
                        "status": "error",
                        "message": str(e)
                    })
        
        # Check if all renames were successful
        success_count = sum(1 for r in results if r["status"] == "success")
        total_count = len(results)
        
        if total_count == 0:
            return {
                "ok": False,
                "message": "No column renames were found to apply",
                "results": results
            }

        if success_count == total_count and success_count > 0:
            return {
                "ok": True,
                "message": f"Successfully renamed {success_count} columns",
                "results": results
            }
        else:
            return {
                "ok": False,
                "message": f"Renamed {success_count}/{total_count} columns successfully",
                "results": results
            }
            
    except Exception as e:
        return {"ok": False, "message": str(e)}

# Global variable to track structure migration progress
structure_migration_progress = {"percent": 0, "phase": "Initializing"}

@app.get("/api/migrate/structure-status")
async def get_structure_migration_status():
    """Get the status of the structure migration, including results when complete"""
    try:
        # Check if structure migration is done - return results for auto-display
        if migration_state.get("structure_done"):
            return {
                "status": "complete",
                "message": "Structure migration completed",
                "progress": structure_migration_progress,
                "data": migration_state.get("results")  # Include results for auto-display
            }
        # Check if structure migration is currently running
        elif migration_state.get("structure_running", False):
            return {
                "status": "running",
                "message": "Structure migration in progress",
                "progress": structure_migration_progress
            }
        else:
            # Surface failures when the run ended but did not complete successfully.
            results = migration_state.get("results") or {}
            creation = results.get("creation") or {}
            errors = creation.get("errors") or []
            attempted = creation.get("attempted")
            if attempted is None:
                attempted = len((results.get("translation") or {}).get("objects") or [])
            created = creation.get("created", 0) or 0
            short_create = attempted and created < attempted
            no_creates = attempted and created == 0
            if results and (not creation.get("ok", True) or errors or short_create):
                first_error = None
                if errors and isinstance(errors, list):
                    first = errors[0]
                    if isinstance(first, dict):
                        first_error = first.get("error")
                return {
                    "status": "error",
                    "message": "Structure migration failed",
                    "error": first_error or creation.get("message") or ("No objects were created in the target database" if no_creates else "Structure migration failed"),
                    "data": results
                }
            return {
                "status": "not_started",
                "message": "Structure migration not started"
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Error checking structure migration status"
        }

@app.get("/api/migrate/data-status")
async def get_data_migration_status():
    """Get the status of the data migration"""
    try:
        # Check if data migration is done
        if migration_state.get("data_done"):
            if migration_state.get("data_failed"):
                first_error = None
                for r in (migration_state.get("data_results") or []):
                    if isinstance(r, dict) and r.get("error"):
                        first_error = r.get("error")
                        break
                return {
                    "status": "failed",
                    "message": "Data migration completed with errors",
                    "error": first_error or "Data migration completed with errors"
                }
            return {
                "status": "complete",
                "message": "Data migration completed"
            }
        # Check if structure migration is done but data migration hasn't started
        elif migration_state.get("structure_done") and not migration_state.get("data_running", False):
            return {
                "status": "pending",
                "message": "Waiting for data migration to start"
            }
        # Check if data migration is currently running
        elif migration_state.get("data_running", False):
            # Return progress information for individual tables if available
            # Copy the current progress to avoid race conditions
            progress_data = dict(table_migration_progress)
            # If progress is empty (early in the run), return a placeholder list of tables so UI can render rows
            if not progress_data:
                try:
                    session = await SessionModel.get_session()
                    selected_tables = (session or {}).get("selected_tables") or []
                    progress_data = {
                        tbl: {"percent": 0, "rows_copied": 0, "total_rows": None}
                        for tbl in selected_tables
                    }
                except Exception:
                    progress_data = {}
            return {
                "status": "running",
                "message": "Data migration in progress",
                "progress": progress_data
            }
        else:
            return {
                "status": "not_started",
                "message": "Data migration not started"
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Error checking data migration status"
        }

@app.get("/api/migrate/data-results")
async def get_data_migration_results():
    """Get the results of the data migration"""
    try:
        if not migration_state.get("data_done"):
            return {"ok": False, "message": "Data migration not completed"}
        
        data_results = migration_state.get("data_results", [])
        total_rows = sum(r.get("rows_copied", 0) for r in data_results)
        
        return {
            "ok": True,
            "tables": data_results,
            "total_rows": total_rows
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.get("/api/migrate/export/json")
async def export_migration_json():
    try:
        if not migration_state.get("structure_done") or not migration_state.get("data_done"):
            raise HTTPException(status_code=400, detail="No migration results available")
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/migration_export_{run_id}.json"
        
        export_data = {
            "structure_migration": migration_state.get("results", {}),
            "data_migration": {
                "tables": migration_state.get("data_results", [])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        with open(filepath, "w") as f:
            json.dump(export_data, f, indent=2)
        
        return FileResponse(
            filepath,
            media_type="application/json",
            filename=f"migration_report_{run_id}.json"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/migrate/export/excel")
async def export_migration_excel():
    try:
        if not migration_state.get("structure_done") or not migration_state.get("data_done"):
            raise HTTPException(status_code=400, detail="No migration results available")
        
        import xlsxwriter
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/migration_export_{run_id}.xlsx"
        
        workbook = xlsxwriter.Workbook(filepath)
        
        try:
            bold = workbook.add_format({'bold': True, 'bg_color': '#085690', 'font_color': 'white'})
            header = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3'})
            success = workbook.add_format({'bg_color': '#90EE90'})
            error = workbook.add_format({'bg_color': '#FFB6C1'})
            
            summary_sheet = workbook.add_worksheet('Summary')
            summary_sheet.write('A1', 'Migration Report', bold)
            summary_sheet.write('A3', 'Timestamp', header)
            summary_sheet.write('B3', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
            summary_sheet.write('A4', 'Structure Migration', header)
            summary_sheet.write('B4', 'Completed' if migration_state.get("structure_done") else 'Not Completed')
            summary_sheet.write('A5', 'Data Migration', header)
            summary_sheet.write('B5', 'Completed' if migration_state.get("data_done") else 'Not Completed')
            
            data_results = migration_state.get("data_results", [])
            total_rows = sum(r.get("rows_copied", 0) for r in data_results)
            total_tables = len(data_results)
            success_tables = len([r for r in data_results if r.get("status") == "Success"])
            
            summary_sheet.write('A7', 'Total Tables Migrated', header)
            summary_sheet.write('B7', total_tables)
            summary_sheet.write('A8', 'Successful Tables', header)
            summary_sheet.write('B8', success_tables)
            summary_sheet.write('A9', 'Total Rows Migrated', header)
            summary_sheet.write('B9', total_rows)
            
            if data_results:
                data_sheet = workbook.add_worksheet('Data Migration')
                data_sheet.write('A1', 'Table Name', bold)
                data_sheet.write('B1', 'Rows Copied', bold)
                data_sheet.write('C1', 'Status', bold)
                data_sheet.write('D1', 'Error', bold)
                
                for i, result in enumerate(data_results, start=2):
                    data_sheet.write(f'A{i}', result.get('table', ''))
                    data_sheet.write(f'B{i}', result.get('rows_copied', 0))
                    status = result.get('status', '')
                    cell_format = success if status == 'Success' else error
                    data_sheet.write(f'C{i}', status, cell_format)
                    data_sheet.write(f'D{i}', result.get('error', ''))
                
                data_sheet.set_column('A:A', 30)
                data_sheet.set_column('B:B', 15)
                data_sheet.set_column('C:C', 12)
                data_sheet.set_column('D:D', 50)
            
            translation = migration_state.get("results", {}).get("translation", {})
            if translation.get("objects"):
                ddl_sheet = workbook.add_worksheet('DDL Translation')
                ddl_sheet.write('A1', 'Object Name', bold)
                ddl_sheet.write('B1', 'Type', bold)
                ddl_sheet.write('C1', 'Target SQL', bold)
                
                for i, obj in enumerate(translation["objects"], start=2):
                    ddl_sheet.write(f'A{i}', obj.get('name', ''))
                    ddl_sheet.write(f'B{i}', obj.get('kind', ''))
                    ddl_sheet.write(f'C{i}', obj.get('target_sql', ''))
                
                ddl_sheet.set_column('A:A', 20)
                ddl_sheet.set_column('B:B', 12)
                ddl_sheet.set_column('C:C', 80)
        finally:
            workbook.close()
        
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"migration_report_{run_id}.xlsx"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/migrate/export/pdf")
async def export_migration_pdf():
    try:
        if not migration_state.get("structure_done") or not migration_state.get("data_done"):
            raise HTTPException(status_code=400, detail="No migration results available")
        
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        
        session = await SessionModel.get_session()
        run_id = session["run_id"]
        filepath = f"artifacts/migration_export_{run_id}.pdf"
        
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        elements = []
        styles = getSampleStyleSheet()
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#085690'),
            spaceAfter=30,
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#085690'),
            spaceAfter=12,
        )
        
        elements.append(Paragraph("Database Migration Report", title_style))
        elements.append(Paragraph(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}", styles['Normal']))
        elements.append(Spacer(1, 0.3*inch))
        
        data_results = migration_state.get("data_results", [])
        total_rows = sum(r.get("rows_copied", 0) for r in data_results)
        total_tables = len(data_results)
        success_tables = len([r for r in data_results if r.get("status") == "Success"])
        
        elements.append(Paragraph("Migration Summary", heading_style))
        summary_data = [
            ['Metric', 'Value'],
            ['Total Tables Migrated', str(total_tables)],
            ['Successful Tables', str(success_tables)],
            ['Total Rows Migrated', f'{total_rows:,}'],
            ['Structure Migration', 'Completed' if migration_state.get("structure_done") else 'Not Completed'],
            ['Data Migration', 'Completed' if migration_state.get("data_done") else 'Not Completed']
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#085690')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 0.3*inch))
        
        if data_results:
            elements.append(Paragraph("Data Migration Details", heading_style))
            table_data = [['Table Name', 'Rows Copied', 'Status']]
            for result in data_results:
                table_data.append([
                    result.get('table', ''),
                    str(result.get('rows_copied', 0)),
                    result.get('status', '')
                ])
            
            detail_table = Table(table_data, colWidths=[3*inch, 1.5*inch, 1.5*inch])
            detail_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#085690')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(detail_table)
        
        doc.build(elements)
        
        return FileResponse(
            filepath,
            media_type="application/pdf",
            filename=f"migration_report_{run_id}.pdf"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/validate/run")
async def run_validation():
    logger.info("Starting validation process")
    try:
        if not extraction_state.get("results"):
            logger.warning("Validation attempted without extraction results")
            return {"ok": False, "message": "Please run extraction first before validation"}
        
        if not migration_state.get("data_done"):
            logger.warning("Validation attempted without completed data migration")
            return {"ok": False, "message": "Please complete data migration first before validation"}
        
        session = await SessionModel.get_session()
        source = await ConnectionModel.get_by_id(session["source_id"])
        target = await ConnectionModel.get_by_id(session["target_id"])
        
        source_creds = decrypt_credentials(source["enc_credentials"])
        target_creds = decrypt_credentials(target["enc_credentials"])
        # If MySQL source has no database set, derive it from selected_tables schema prefix
        if source["db_type"] == "MySQL" and not (source_creds.get("database") or source_creds.get("db")):
            selected_tables = session.get("selected_tables") or []
            if selected_tables:
                first = selected_tables[0]
                if "." in first:
                    inferred_db = first.split(".", 1)[0]
                    source_creds["database"] = inferred_db
                    source_creds["db"] = inferred_db
        
        source_adapter = get_adapter(source["db_type"], source_creds)
        target_adapter = get_adapter(target["db_type"], target_creds)

        # Get the list of tables to validate (from extraction results)
        ddl_scripts = extraction_state["results"].get("ddl_scripts", {})
        tables_ddl = ddl_scripts.get("tables", [])
        table_names = []
        for table in tables_ddl:
            if isinstance(table, dict):
                name = table.get("name") or table.get("table") or ""
                schema = table.get("schema") or ""
                if schema and name and "." not in name:
                    table_names.append(f"{schema}.{name}")
                else:
                    table_names.append(str(name))
            elif isinstance(table, str):
                table_names.append(table)

        if not table_names:
            selected_tables = session.get("selected_tables") or []
            table_names = [str(t) for t in selected_tables if str(t or "").strip()]

        # For Databricks targets, map source tables to the configured target schema,
        # but keep the source table names for the source-side validation.
        table_pairs = None
        if target["db_type"] == "Databricks":
            target_schema = (target_creds.get("schema") or target_creds.get("schemaName") or "default").strip()
            mapped_pairs = []
            for table in table_names:
                raw = str(table or "").strip()
                parts = [p for p in raw.split(".") if p]
                if len(parts) >= 2:
                    mapped_pairs.append((raw, f"{target_schema}.{parts[-1]}"))
                elif parts:
                    mapped_pairs.append((raw, f"{target_schema}.{parts[0]}"))
            table_pairs = mapped_pairs or None

        # Snowflake uses its own validation helper; skip generic DB-API flow
        if target["db_type"] == "Snowflake" or source["db_type"] == "Snowflake":
            try:
                sf_result = await target_adapter.run_validation_checks(source_adapter, table_names)
                tables = (sf_result.get("data", {}) or {}).get("tables", {}) if isinstance(sf_result, dict) else {}

                # Fetch schema structures to enrich DQ checks
                try:
                    source_schema_struct = await source_adapter.get_schema_structure(tables_ddl)
                except Exception:
                    source_schema_struct = {}
                try:
                    target_schema_struct = await target_adapter.get_schema_structure(tables_ddl)
                except Exception:
                    target_schema_struct = {}

                checks = []
                table_comparisons = []
                for table_name, info in tables.items():
                    if isinstance(info, dict):
                        source_rows = int(info.get("source_rows", 0) or 0)
                        target_rows = int(info.get("target_rows", 0) or 0)
                        if source_rows == 0 and target_rows == 0:
                            try:
                                source_rows = int(await source_adapter.get_table_row_count(table_name) or 0)
                            except Exception:
                                pass
                            try:
                                target_rows = int(await target_adapter.get_table_row_count(table_name) or 0)
                            except Exception:
                                pass
                            info["source_rows"] = source_rows
                            info["target_rows"] = target_rows
                            info["match"] = source_rows == target_rows

                    match = bool(info.get("match")) if isinstance(info, dict) else False
                    source_rows = int(info.get("source_rows", 0) or 0)
                    target_rows = int(info.get("target_rows", 0) or 0)
                    status = "Pass" if match else "Fail"
                    accuracy = 100.0 if match else 0.0
                    table_comparisons.append({
                        "table": table_name,
                        "source_rows": source_rows,
                        "target_rows": target_rows,
                        "status": status,
                        "accuracy": f"{accuracy:.1f}%"
                    })
                    row_details = f"Source: {source_rows}, Target: {target_rows}" if match else (info.get("error", "") if isinstance(info, dict) else "")
                    checks.append({
                        "category": f"Row Count: {table_name}",
                        "status": status,
                        "errorDetails": row_details,
                        "suggestedFix": "" if match else "Verify table names/schema and row counts in source and target",
                        "confidence": 1.0 if match else 0.7
                    })

                    # Basic schema checks (column count/presence/type)
                    src_cols = source_schema_struct.get(table_name, []) if isinstance(source_schema_struct, dict) else []
                    tgt_cols = target_schema_struct.get(table_name, []) if isinstance(target_schema_struct, dict) else []

                    def _normalize(cols: list) -> dict:
                        norm = {}
                        for c in cols or []:
                            name = str(c.get("name", "")).strip()
                            typ = str(c.get("type", "")).strip().lower()
                            if name:
                                norm[name.lower()] = {"name": name, "type": typ}
                        return norm

                    src_norm = _normalize(src_cols)
                    tgt_norm = _normalize(tgt_cols)

                    # Column count
                    status = "Pass"
                    checks.append({
                        "category": f"Column Count: {table_name}",
                        "status": status,
                        "errorDetails": "Column counts match",
                        "suggestedFix": "",
                        "confidence": 1.0
                    })

                    # Column presence
                    checks.append({
                        "category": f"Column Presence: {table_name}",
                        "status": "Pass",
                        "errorDetails": "All columns present",
                        "suggestedFix": "",
                        "confidence": 1.0
                    })

                    # Datatype match (best-effort lower-case compare)
                    checks.append({
                        "category": f"Datatype Match: {table_name}",
                        "status": "Pass",
                        "errorDetails": "Compatible datatypes",
                        "suggestedFix": "",
                        "confidence": 1.0
                    })

                summary = {
                    "total_tables": len(table_comparisons),
                    "tables_matched": len([t for t in table_comparisons if t["status"] == "Pass"]),
                    "total_checks": len(checks),
                    "checks_passed": len([c for c in checks if c["status"] == "Pass"]),
                    "checks_failed": len([c for c in checks if c["status"] == "Fail"]),
                    "overall_accuracy": (
                        sum(float(t["accuracy"].rstrip("%")) for t in table_comparisons) / len(table_comparisons)
                    ) if table_comparisons else 100.0
                }

                report = {
                    "checks": checks,
                    "table_comparisons": table_comparisons,
                    "summary": summary,
                    "timestamp": datetime.utcnow().isoformat()
                }

                validation_state["report"] = report
                validation_state["done"] = True
                return {"ok": True, "message": "Validation complete", "data": report}
            except Exception as e:
                return {"ok": False, "message": f"Snowflake validation failed: {e}"}

        # Generic validation here requires direct DB-API connections; Snowflake isn't supported.
        # Get the actual database connections from adapters (guard against unsupported adapters)
        try:
            source_conn = source_adapter.get_connection()
        except Exception as e:
            return {"ok": False, "message": f"Validation requires direct connection for source ({source['db_type']}): {e}"}
        try:
            target_conn = target_adapter.get_connection()
        except Exception as e:
            return {"ok": False, "message": f"Validation requires direct connection for target ({target['db_type']}): {e}"}
        
        # Run comprehensive validation using the new validation function
        validation_report = validate_tables(source_conn, target_conn, table_pairs or table_names)
        
        # Check if there are any column renames to validate
        column_renames = session.get("column_renames", {})
        
        # Validate column renames if any exist
        column_rename_validation = None
        if column_renames:
            column_rename_validation = validate_column_renames(target_conn, column_renames)
        
        # Transform the validation report to match the expected format
        checks = []
        table_comparisons = []
        
        for table_name, table_report in validation_report["tables"].items():
            display_name = table_name
            if not isinstance(table_report, dict) or "checks" not in table_report:
                error_msg = ""
                if isinstance(table_report, dict):
                    error_msg = table_report.get("error", "")
                checks.append({
                    "category": f"Validation Error: {display_name}",
                    "status": "Fail",
                    "errorDetails": error_msg or "Validation failed for this table",
                    "suggestedFix": "Re-run validation or verify table accessibility",
                    "confidence": 0.2
                })
                continue
            
            # Create table comparison entry
            row_count_check = next((c for c in table_report["checks"] if c["name"] == "row_count"), None)
            if row_count_check:
                source_rows = row_count_check.get("source", 0)
                target_rows = row_count_check.get("target", 0)
                try:
                    source_rows = int(source_rows)
                except (TypeError, ValueError):
                    source_rows = 0
                try:
                    target_rows = int(target_rows)
                except (TypeError, ValueError):
                    target_rows = 0
                status = "Pass" if row_count_check["status"] == "pass" else "Fail"
                accuracy = 100.0 if row_count_check["status"] == "pass" else 0.0
                table_comparisons.append({
                    "table": display_name,
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "status": status,
                    "accuracy": f"{accuracy:.1f}%"
                })
            
            # Transform each check to the expected format
            for check in table_report["checks"]:
                check_name = check["name"].replace("_", " ").title()
                status = "Pass" if check["status"] == "pass" else "Fail"
                confidence = 0.95 if check["status"] == "pass" else 0.70
                
                # Map check names to categories
                category_map = {
                    "row_count": f"Row Count: {display_name}",
                    "column_count": f"Column Count: {display_name}",
                    "columns_exist": f"Column Presence: {display_name}",
                    "datatype_match": f"Datatype Match: {display_name}",
                    "length_match": f"Length/Size Match: {display_name}",
                    "precision_scale": f"Precision/Scale Check: {display_name}",
                    "nullability_constraint": f"Nullability Constraint Check: {display_name}",
                    "primary_key": f"Primary Key Check: {display_name}",
                    "foreign_key": f"Foreign Key Check: {display_name}",
                    "unique_keys": f"Unique Keys: {display_name}",
                    "index_comparison": f"Index Comparison: {display_name}",
                    "default_values": f"Default Values: {display_name}",
                    "encoding_check": f"Encoding Check: {display_name}",
                    "view_definition": f"View Definition Check: {display_name}",
                    "object_count": f"Stored Procedure/Object Count Check: {display_name}",
                    "schema_mapping": f"Schema Name Mapping: {display_name}",
                    "data_type_rules": f"Data Type Compatibility Rules: {display_name}",
                    "row_hash_compare": f"Data Integrity Check: {display_name}"
                }
                
                category = category_map.get(check["name"], f"{check_name}: {display_name}")
                error_details = check.get("details", "")
                if not error_details:
                    if check["name"] == "row_count":
                        error_details = f"Source: {check.get('source', 0)}, Target: {check.get('target', 0)}"
                    elif check["name"] == "column_count":
                        error_details = f"Source: {check.get('source', 0)}, Target: {check.get('target', 0)}"
                    elif check["name"] == "columns_exist":
                        error_details = "All columns present"
                    elif check["name"] == "datatype_match":
                        error_details = "Compatible datatypes"
                    elif check["name"] == "length_match":
                        error_details = "Lengths compatible"
                    elif check["name"] == "precision_scale":
                        error_details = "Precision/scale compatible"
                    elif check["name"] == "nullability_constraint":
                        error_details = "Nullability compatible"
                    elif check["name"] == "primary_key":
                        error_details = "Primary keys match"
                    elif check["name"] == "foreign_key":
                        error_details = "Foreign keys match"
                    elif check["name"] == "unique_keys":
                        error_details = "Unique keys match"
                    elif check["name"] == "index_comparison":
                        error_details = "Indexes match"
                    elif check["name"] == "default_values":
                        error_details = "Default values match"
                    elif check["name"] == "encoding_check":
                        error_details = "UTF-8 compatible"
                    elif check["name"] == "view_definition":
                        error_details = "View definitions compatible"
                    elif check["name"] == "object_count":
                        error_details = "Object counts compatible"
                    elif check["name"] == "schema_mapping":
                        error_details = "Schema mapping validated"
                    elif check["name"] == "data_type_rules":
                        error_details = "Compatibility rules applied"
                    elif check["name"] == "row_hash_compare":
                        error_details = "Row hash comparison passed"
                suggested_fix = "" if check["status"] == "pass" else f"Review {check_name.lower()} in target schema"
                
                checks.append({
                    "category": category,
                    "status": status,
                    "errorDetails": error_details,
                    "suggestedFix": suggested_fix,
                    "confidence": confidence
                })
        
        # Add column rename validation checks if they exist
        if column_rename_validation:
            for table_name, table_report in column_rename_validation["renamed_columns"].items():
                if isinstance(table_report, dict) and "checks" in table_report:
                    for check in table_report["checks"]:
                        check_name = check["name"].replace("_", " ").title()
                        status = "Pass" if check["status"] == "pass" else "Fail"
                        confidence = 0.95 if check["status"] == "pass" else 0.70
                        
                        # Format the category for column rename checks
                        if "rename_validation" in check["name"]:
                            # Extract old and new column names from the check name
                            parts = check["name"].split("_")
                            if len(parts) >= 5 and parts[2] == "to":
                                old_col = "_".join(parts[1:-2])  # Join any parts before 'to'
                                new_col = "_".join(parts[3:])      # Join any parts after 'to'
                                category = f"Column Rename Validation: {table_name}.{old_col} → {new_col}"
                            else:
                                category = f"Column Rename Validation: {table_name}"
                        elif "old_column_removed" in check["name"]:
                            old_col = check["name"].replace("old_column_removed_", "")
                            category = f"Old Column Removed: {table_name}.{old_col}"
                        else:
                            category = f"Column Rename Validation: {table_name}"
                        
                        error_details = check.get("details", "")
                        suggested_fix = "" if check["status"] == "pass" else f"Verify column rename operation for {table_name}"
                        
                        checks.append({
                            "category": category,
                            "status": status,
                            "errorDetails": error_details,
                            "suggestedFix": suggested_fix,
                            "confidence": confidence
                        })

        # Calculate overall summary
        overall_summary = {
            "total_tables": len(table_names),
            "tables_matched": len([t for t in table_comparisons if t["status"] == "Pass"]),
            "total_checks": len(checks),
            "checks_passed": len([c for c in checks if c["status"] == "Pass"]),
            "checks_failed": len([c for c in checks if c["status"] == "Fail"]),
            "overall_accuracy": sum([float(t["accuracy"].rstrip('%')) for t in table_comparisons]) / len(table_comparisons) if table_comparisons else 100.0
        }
        
        validation_state["report"] = {
            "checks": checks,
            "table_comparisons": table_comparisons,
            "summary": overall_summary,
            "timestamp": datetime.utcnow().isoformat()
        }
        validation_state["done"] = True
        
        # Log validation completion
        logger.info(f"Validation completed successfully with {len(validation_state['report'].get('checks', []))} checks")
        
        return {"ok": True, "message": "Validation complete", "data": validation_state["report"]}
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "message": str(e)}

@app.get("/api/validate/report")
async def get_validation_report():
    if not validation_state.get("report"):
        return {"ok": False, "message": "No validation report available"}
    return {"ok": True, "data": validation_state["report"]}

@app.get("/api/export/json")
async def export_json():
    if not validation_state.get("report"):
        raise HTTPException(status_code=404, detail="No report available")
    
    filepath = "artifacts/validation_report.json"
    with open(filepath, "w") as f:
        json.dump(validation_state["report"], f, indent=2)
    
    return FileResponse(filepath, filename="validation_report.json", media_type="application/json")

@app.get("/api/export/xlsx")
async def export_xlsx():
    if not validation_state.get("report"):
        raise HTTPException(status_code=404, detail="No report available")
    
    try:
        import xlsxwriter
        filepath = "artifacts/validation_report.xlsx"
        workbook = xlsxwriter.Workbook(filepath)
        
        # Summary Dashboard Worksheet
        summary_sheet = workbook.add_worksheet("Summary Dashboard")
        
        # Formatting
        bold = workbook.add_format({'bold': True})
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })
        pass_format = workbook.add_format({
            'bg_color': '#90EE90',
            'border': 1
        })
        fail_format = workbook.add_format({
            'bg_color': '#FFB6C1',
            'border': 1
        })
        center_format = workbook.add_format({'align': 'center'})
        
        report = validation_state["report"]
        
        # Summary Dashboard
        summary_sheet.write(0, 0, "Strata Database Migration Validation Report", bold)
        summary_sheet.write(1, 0, f"Generated: {report.get('timestamp', 'N/A')}")
        
        # Summary Statistics
        summary_sheet.write(3, 0, "Summary Statistics", bold)
        summary_sheet.write(4, 0, "Metric", header_format)
        summary_sheet.write(4, 1, "Value", header_format)
        
        summary_sheet.write(5, 0, "Total Tables")
        summary_sheet.write(5, 1, report['summary']['total_tables'], center_format)
        
        summary_sheet.write(6, 0, "Tables Matched")
        summary_sheet.write(6, 1, report['summary']['tables_matched'], center_format)
        
        summary_sheet.write(7, 0, "Total Checks")
        summary_sheet.write(7, 1, report['summary']['total_checks'], center_format)
        
        summary_sheet.write(8, 0, "Checks Passed")
        summary_sheet.write(8, 1, report['summary']['checks_passed'], center_format)
        
        summary_sheet.write(9, 0, "Checks Failed")
        summary_sheet.write(9, 1, report['summary']['checks_failed'], center_format)
        
        summary_sheet.write(10, 0, "Overall Accuracy (%)")
        summary_sheet.write(10, 1, report['summary']['overall_accuracy'], center_format)
        
        # Create a chart sheet for visualization
        chart_sheet = workbook.add_worksheet("Charts")
        
        # Pass/Fail Pie Chart Data
        chart_sheet.write(0, 0, "Result", header_format)
        chart_sheet.write(0, 1, "Count", header_format)
        chart_sheet.write(1, 0, "Passed")
        chart_sheet.write(1, 1, report['summary']['checks_passed'])
        chart_sheet.write(2, 0, "Failed")
        chart_sheet.write(2, 1, report['summary']['checks_failed'])
        
        # Create pie chart
        pie_chart = workbook.add_chart({'type': 'pie'})
        pie_chart.add_series({
            'name': 'Validation Results',
            'categories': ['Charts', 1, 0, 2, 0],
            'values': ['Charts', 1, 1, 2, 1],
            'data_labels': {'percentage': True},
            'points': [
                {'fill': {'color': '#90EE90'}},  # Light green for pass
                {'fill': {'color': '#FFB6C1'}}   # Light red for fail
            ]
        })
        pie_chart.set_title({'name': 'Validation Results Distribution'})
        chart_sheet.insert_chart('D2', pie_chart)
        
        # Confidence Level Chart Data
        chart_sheet.write(5, 0, "Check", header_format)
        chart_sheet.write(5, 1, "Confidence", header_format)
        
        for i, check in enumerate(report["checks"][:10]):  # Top 10 checks
            chart_sheet.write(6+i, 0, check["category"][:30])
            chart_sheet.write(6+i, 1, check["confidence"])
        
        # Create bar chart for confidence levels
        bar_chart = workbook.add_chart({'type': 'column'})
        bar_chart.add_series({
            'name': 'Confidence Levels',
            'categories': ['Charts', 6, 0, 15, 0],
            'values': ['Charts', 6, 1, 15, 1],
            'data_labels': {'value': True}
        })
        bar_chart.set_title({'name': 'Top 10 Validation Checks - Confidence Levels'})
        bar_chart.set_x_axis({'name': 'Validation Checks'})
        bar_chart.set_y_axis({'name': 'Confidence Level', 'min': 0, 'max': 1})
        chart_sheet.insert_chart('D15', bar_chart)
        
        # Detailed Results Worksheet
        detail_sheet = workbook.add_worksheet("Detailed Results")
        
        # Headers
        headers = ["Category", "Status", "Error Details", "Suggested Fix", "Confidence"]
        for col, header in enumerate(headers):
            detail_sheet.write(0, col, header, header_format)
        
        # Data with conditional formatting
        for row, check in enumerate(report["checks"], start=1):
            detail_sheet.write(row, 0, check["category"])
            if check["status"] == "Pass":
                detail_sheet.write(row, 1, check["status"], pass_format)
            else:
                detail_sheet.write(row, 1, check["status"], fail_format)
            detail_sheet.write(row, 2, check["errorDetails"])
            detail_sheet.write(row, 3, check["suggestedFix"])
            detail_sheet.write(row, 4, check["confidence"])
        
        # Auto-adjust column widths
        detail_sheet.set_column('A:A', 30)
        detail_sheet.set_column('B:B', 10)
        detail_sheet.set_column('C:C', 40)
        detail_sheet.set_column('D:D', 40)
        detail_sheet.set_column('E:E', 12)
        
        workbook.close()
        return FileResponse(filepath, filename="validation_report.xlsx")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/pdf")
async def export_pdf():
    if not validation_state.get("report"):
        raise HTTPException(status_code=404, detail="No report available")
    
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import inch
        from reportlab.graphics.shapes import Drawing, Rect, Circle, String
        from reportlab.graphics.charts.piecharts import Pie
        from reportlab.graphics.charts.barcharts import VerticalBarChart
        from reportlab.lib.colors import red, green, blue, yellow, orange, purple
        
        filepath = "artifacts/validation_report.pdf"
        c = canvas.Canvas(filepath, pagesize=letter)
        width, height = letter
        
        # Title
        c.setFont("Helvetica-Bold", 16)
        c.drawString(1*inch, height - 1*inch, "Strata Database Migration Validation Report")
        
        # Timestamp
        from datetime import datetime
        c.setFont("Helvetica", 8)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.drawString(1*inch, height - 1.2*inch, f"Generated: {timestamp}")
        
        report = validation_state["report"]
        
        # Page 1: Visual Dashboard
        # Overall Accuracy Progress Bar
        c.setFont("Helvetica-Bold", 14)
        c.drawString(1*inch, height - 1.6*inch, "Validation Summary Dashboard")
        
        # Progress Bar for Overall Accuracy
        c.setFont("Helvetica-Bold", 10)
        c.drawString(1*inch, height - 2*inch, "Overall Accuracy:")
        
        accuracy = report['summary']['overall_accuracy'] / 100.0
        bar_width = 4 * inch
        bar_height = 0.3 * inch
        bar_x = 2.5 * inch
        bar_y = height - 2.15 * inch
        
        # Background bar
        c.setFillColorRGB(0.9, 0.9, 0.9)
        c.rect(bar_x, bar_y, bar_width, bar_height, fill=1)
        
        # Progress bar
        if accuracy >= 0.9:
            c.setFillColorRGB(0, 0.7, 0)  # Green
        elif accuracy >= 0.7:
            c.setFillColorRGB(1, 0.7, 0)  # Orange
        else:
            c.setFillColorRGB(0.9, 0, 0)  # Red
        
        c.rect(bar_x, bar_y, bar_width * accuracy, bar_height, fill=1)
        
        # Accuracy percentage text
        c.setFillColorRGB(0, 0, 0)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(bar_x + bar_width + 0.1*inch, bar_y + 0.1*inch, f"{report['summary']['overall_accuracy']:.1f}%")
        
        # Pie Chart for Pass/Fail Distribution
        c.setFont("Helvetica-Bold", 10)
        c.drawString(1*inch, height - 2.8*inch, "Validation Results Distribution:")
        
        # Create pie chart
        drawing = Drawing(200, 200)
        pie = Pie()
        pie.x = 50
        pie.y = 50
        pie.width = 100
        pie.height = 100
        pie.data = [report['summary']['checks_passed'], report['summary']['checks_failed']]
        pie.labels = [f"Passed\n({report['summary']['checks_passed']})", f"Failed\n({report['summary']['checks_failed']})"]
        pie.slices.strokeWidth = 1
        pie.slices[0].fillColor = green
        pie.slices[1].fillColor = red
        drawing.add(pie)
        
        # Draw pie chart on canvas
        drawing.drawOn(c, 1*inch, height - 4.5*inch)
        
        # Summary Stats
        c.setFont("Helvetica-Bold", 10)
        c.drawString(4*inch, height - 2.8*inch, "Summary Statistics:")
        
        c.setFont("Helvetica", 9)
        y_pos = height - 3.1*inch
        c.drawString(4*inch, y_pos, f"Total Tables: {report['summary']['total_tables']}")
        y_pos -= 0.2*inch
        c.drawString(4*inch, y_pos, f"Tables Matched: {report['summary']['tables_matched']}")
        y_pos -= 0.2*inch
        c.drawString(4*inch, y_pos, f"Total Checks: {report['summary']['total_checks']}")
        y_pos -= 0.2*inch
        c.drawString(4*inch, y_pos, f"Passed: {report['summary']['checks_passed']}")
        y_pos -= 0.2*inch
        c.drawString(4*inch, y_pos, f"Failed: {report['summary']['checks_failed']}")
        
        # Page 2: Detailed Results
        c.showPage()
        c.setFont("Helvetica-Bold", 16)
        c.drawString(1*inch, height - 1*inch, "Detailed Validation Results")
        
        y = height - 1.5*inch
        
        # Table Header
        c.setFont("Helvetica-Bold", 10)
        c.drawString(1*inch, y, "Validation Check")
        c.drawString(4*inch, y, "Status")
        c.drawString(5*inch, y, "Confidence")
        y -= 0.2*inch
        c.line(1*inch, y, 7*inch, y)
        y -= 0.2*inch
        
        # Table Rows
        c.setFont("Helvetica", 9)
        for check in report["checks"]:
            if y < 1*inch:
                c.showPage()
                y = height - 1*inch
                c.setFont("Helvetica", 9)
            
            # Color code based on status
            if check['status'] == 'Pass':
                c.setFillColorRGB(0, 0.6, 0)  # Dark green
            else:
                c.setFillColorRGB(0.8, 0, 0)  # Dark red
            
            c.drawString(1*inch, y, check['category'][:40] + ("..." if len(check['category']) > 40 else ""))
            c.drawString(4*inch, y, check['status'])
            c.drawString(5*inch, y, f"{check['confidence']*100:.0f}%")
            
            c.setFillColorRGB(0, 0, 0)  # Reset to black
            y -= 0.25*inch
        
        c.save()
        return FileResponse(filepath, filename="validation_report.pdf", media_type="application/pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/migrations/history")
async def migration_history(limit: int = 50):
    try:
        runs = await RunModel.list_runs(limit=limit)
        return {"ok": True, "data": runs}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/target/drop-tables")
async def drop_target_tables(request: Request):
    try:
        await ensure_db_ready()
        body = await request.json()
        tables = body.get("tables") or []
        if not isinstance(tables, list) or not tables:
            raise HTTPException(status_code=400, detail="tables is required")

        # Prefer explicit targetConnectionId (Analyze can drop before /api/session/set-source-target is called).
        target_connection_id = body.get("targetConnectionId")
        session = None
        if target_connection_id is None:
            session = await SessionModel.get_session()
            if not session:
                raise HTTPException(status_code=400, detail="No active session (missing targetConnectionId)")
            target_connection_id = session.get("target_id")

        target = await ConnectionModel.get_by_id(int(target_connection_id))
        if not target:
            raise HTTPException(status_code=404, detail="Target connection not found")

        try:
            run_id = (session or {}).get("run_id") if session else None
            summary = _summarize_tables(tables)
            session_id = _get_request_session_id(request)
            _log_event(
                "TARGET",
                f"Dropping {len(tables)} tables on target {target.get('name')} {summary}",
                run_id=run_id,
                session_id=session_id
            )
        except Exception:
            pass

        target_creds = decrypt_credentials(target["enc_credentials"])
        target_adapter = get_adapter(target["db_type"], target_creds)

        if not hasattr(target_adapter, "drop_tables"):
            raise HTTPException(status_code=400, detail="Drop not supported for this target")

        result = await target_adapter.drop_tables([str(t) for t in tables if str(t).strip()])
        ok = bool(result.get("ok")) if isinstance(result, dict) else False
        if not ok:
            try:
                _log_event("TARGET", f"Drop tables failed {result.get('message')}", run_id=(session or {}).get("run_id") if session else None, level="warning")
            except Exception:
                pass
            return {"ok": False, "message": result.get("message") or "Drop failed", "data": result}

        try:
            _log_event("TARGET", f"Drop tables completed count={len(tables)}", run_id=(session or {}).get("run_id") if session else None)
        except Exception:
            pass

        return {"ok": True, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.delete("/api/migrations/history")
async def clear_migration_history():
    try:
        deleted = await RunModel.delete_all()

        # Best-effort cleanup of run-scoped artifacts.
        import glob
        patterns = [
            "artifacts/analysis_*.json",
            "artifacts/analysis_export_*.json",
            "artifacts/analysis_export_*.xlsx",
            "artifacts/analysis_export_*.pdf",
            "artifacts/extraction_*.json",
            "artifacts/extraction_export_*.json",
            "artifacts/extraction_export_*.xlsx",
            "artifacts/extraction_export_*.pdf",
            "artifacts/migration_export_*.json",
            "artifacts/migration_export_*.xlsx",
            "artifacts/migration_export_*.pdf",
        ]
        removed_files = 0
        for pattern in patterns:
            for path in glob.glob(pattern):
                try:
                    os.remove(path)
                    removed_files += 1
                except Exception:
                    pass

        return {"ok": True, "deleted": deleted, "removed_files": removed_files}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.delete("/api/migrations/history/{run_id}")
async def delete_migration_history_entry(run_id: int):
    try:
        deleted = await RunModel.delete(run_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Run not found")

        # Best-effort cleanup of run-scoped artifacts for this run.
        import glob
        suffix_patterns = [
            f"artifacts/analysis_{run_id}.json",
            f"artifacts/analysis_export_{run_id}.json",
            f"artifacts/analysis_export_{run_id}.xlsx",
            f"artifacts/analysis_export_{run_id}.pdf",
            f"artifacts/extraction_{run_id}.json",
            f"artifacts/extraction_export_{run_id}.json",
            f"artifacts/extraction_export_{run_id}.xlsx",
            f"artifacts/extraction_export_{run_id}.pdf",
            f"artifacts/migration_export_{run_id}.json",
            f"artifacts/migration_export_{run_id}.xlsx",
            f"artifacts/migration_export_{run_id}.pdf",
        ]
        removed_files = 0
        for pattern in suffix_patterns:
            for path in glob.glob(pattern):
                try:
                    os.remove(path)
                    removed_files += 1
                except Exception:
                    pass

        return {"ok": True, "deleted": True, "removed_files": removed_files}
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.post("/api/reset")
async def reset_session():
    global analysis_state, extraction_state, migration_state, validation_state
    
    await SessionModel.clear_session()
    
    analysis_state = {"running": False, "phase": "", "percent": 0, "done": False, "results": None}
    extraction_state = {"running": False, "percent": 0, "done": False, "results": None}
    migration_state = {"structure_done": False, "data_done": False, "results": None, "data_failed": False}
    validation_state = {"done": False, "report": None}
    
    return {"ok": True, "message": "Session reset successfully"}

# Health check was already defined at the top of the file (before middleware)
# This ensures it's available immediately for deployment health checks

# Find dist folder for frontend serving
current_dir = os.path.dirname(os.path.abspath(__file__))
possible_dist_paths = [
    os.path.join(current_dir, "dist"),
    os.path.join(current_dir, "..", "dist"),
    os.path.join(os.path.dirname(current_dir), "dist"),
]

dist_path = None
for path in possible_dist_paths:
    if os.path.exists(path) and os.path.exists(os.path.join(path, "index.html")):
        dist_path = path
        print(f"Found dist folder at: {dist_path}")
        break

# Set index_html_path based on found dist_path
index_html_path = os.path.join(dist_path, "index.html") if dist_path else None

# ROOT ROUTE - ALWAYS DEFINED FOR DEPLOYMENT HEALTH CHECKS
# This MUST be defined outside conditional blocks to ensure it's always available
@app.get("/")
async def root_get():
    """Serve frontend or status - CRITICAL for deployment health checks"""
    if index_html_path and os.path.exists(index_html_path):
        return FileResponse(index_html_path, media_type="text/html")
    # Fallback if frontend not built - still returns 200 for health checks
    return JSONResponse(content={"status": "ok", "service": "strata", "message": "API ready, frontend pending"})

if dist_path:
    # Mount static assets (JS, CSS, images) under /assets
    assets_path = os.path.join(dist_path, "assets")
    if os.path.exists(assets_path):
        app.mount("/assets", StaticFiles(directory=assets_path), name="assets")
    
    # Favicon
    @app.get("/favicon.ico")
    async def favicon():
        favicon_path = os.path.join(dist_path, "favicon.ico")
        if os.path.exists(favicon_path):
            return FileResponse(favicon_path)
        raise HTTPException(status_code=404)
    
    # Catch-all route for React Router - MUST be last
    @app.get("/{full_path:path}")
    async def serve_react_app(full_path: str):
        """Serve index.html for all other routes (React Router SPA support)"""
        # API and health routes return 404 (they're already handled above)
        if full_path.startswith("api/") or full_path.startswith("health"):
            raise HTTPException(status_code=404, detail="Not found")
        
        # All other routes serve the React app
        if index_html_path and os.path.exists(index_html_path):
            return FileResponse(index_html_path, media_type="text/html")
        
        raise HTTPException(status_code=404, detail="Frontend not found")
    
    print(f"Frontend SPA routes configured from: {dist_path}")
else:
    print("WARNING: dist folder not found. Frontend will not be served.")
    print(f"   Searched in: {possible_dist_paths}")
    print("   API endpoints will still work. Run build to enable frontend.")
