import os
import json
import asyncio
import time
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import requests
from cryptography.fernet import Fernet
try:
    # Prefer package-relative import when used as part of the backend package
    from .encryption import get_fernet_key
except Exception:
    # Fallback for direct execution contexts
    from backend.encryption import get_fernet_key  # type: ignore

# Load env vars from both project root .env and backend/.env so AI creds are always picked up.
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
load_dotenv(Path(__file__).resolve().parent / ".env")

print("[AI MODULE] Loading AI module")
AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
print(f"[AI MODULE] API Key from env: {'FOUND' if AI_INTEGRATIONS_OPENAI_API_KEY else 'NOT FOUND'}")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
AI_INTEGRATIONS_OPENAI_MODEL = os.environ.get("AI_INTEGRATIONS_OPENAI_MODEL")
print(f"[AI MODULE] Model from env: {AI_INTEGRATIONS_OPENAI_MODEL}")

# Databricks serving endpoint support (used as primary translator when configured).
DATABRICKS_LLM_INVOCATIONS_URL = os.environ.get(
    "DATABRICKS_LLM_INVOCATIONS_URL",
    "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations"
)

def _get_databricks_token() -> str | None:
    # 1) Prefer explicit plaintext env if set (e.g., injected at runtime by secret manager)
    plain = os.environ.get("DATABRICKS_LLM_TOKEN")
    if plain:
        return plain

    # 2) Otherwise attempt to decrypt encrypted env
    enc = os.environ.get("DATABRICKS_LLM_TOKEN_ENC")
    if enc:
        try:
            f = Fernet(get_fernet_key())
            return f.decrypt(enc.encode()).decode()
        except Exception as e:
            print(f"[AI MODULE] Warning: Failed to decrypt DATABRICKS_LLM_TOKEN_ENC: {e}")
            return None
    return None

DATABRICKS_LLM_TOKEN = _get_databricks_token()

DATABRICKS_TIMEOUT_SECONDS = int(os.environ.get("DATABRICKS_LLM_TIMEOUT_SECONDS", "45"))
DATABRICKS_MAX_RETRIES = int(os.environ.get("DATABRICKS_LLM_MAX_RETRIES", "3"))
DATABRICKS_CIRCUIT_FAILURE_THRESHOLD = int(os.environ.get("DATABRICKS_LLM_CIRCUIT_FAILURE_THRESHOLD", "5"))
DATABRICKS_CIRCUIT_OPEN_SECONDS = int(os.environ.get("DATABRICKS_LLM_CIRCUIT_OPEN_SECONDS", "90"))

_databricks_failure_count = 0
_databricks_circuit_open_until = 0.0

client = None
if AI_INTEGRATIONS_OPENAI_API_KEY:
    try:
        print("[AI MODULE] Initializing OpenAI client")
        init_params = {"api_key": AI_INTEGRATIONS_OPENAI_API_KEY}
        if AI_INTEGRATIONS_OPENAI_BASE_URL:
            init_params["base_url"] = AI_INTEGRATIONS_OPENAI_BASE_URL
        client = OpenAI(**init_params)
        print("[AI MODULE] OpenAI client initialized successfully")
    except Exception as e:
        print(f"[AI MODULE] Warning: Could not initialize OpenAI client: {e}")
        client = None
else:
    print("[AI MODULE] No API key found, skipping OpenAI client initialization")

# Use the model from environment variable only if explicitly set.
# No default model is provided; rely on Databricks LLM for translations.
model = AI_INTEGRATIONS_OPENAI_MODEL
print(f"[AI MODULE] Using model: {model if model else 'None (Databricks only)'}")


def _extract_llm_content(result_json: dict) -> str:
    choices = result_json.get("choices") or []
    if choices and isinstance(choices, list):
        first = choices[0] or {}
        message = first.get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        # Some providers return content blocks.
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            if parts:
                return "\n".join(parts)
    # Fallback for raw text payloads
    if isinstance(result_json.get("text"), str):
        return result_json["text"]
    return ""


def _normalize_translation_result(result: dict, default_obj: dict) -> dict:
    if not isinstance(result, dict):
        raise ValueError("Translation result must be a JSON object")

    objects = result.get("objects")
    warnings = result.get("warnings", [])
    if objects is None:
        raise ValueError("Translation JSON missing 'objects'")
    if not isinstance(objects, list):
        raise ValueError("'objects' must be a list")
    if not isinstance(warnings, list):
        warnings = [str(warnings)]

    normalized_objects = []
    for idx, obj in enumerate(objects):
        if not isinstance(obj, dict):
            raise ValueError(f"Object at index {idx} must be a JSON object")
        target_sql = obj.get("target_sql")
        if not isinstance(target_sql, str):
            raise ValueError(f"Object at index {idx} missing string 'target_sql'")
        normalized_objects.append({
            "name": str(obj.get("name") or default_obj.get("name") or f"object_{idx+1}"),
            "kind": str(obj.get("kind") or default_obj.get("kind") or "table"),
            "schema": obj.get("schema") if obj.get("schema") is not None else default_obj.get("schema"),
            "target_sql": target_sql,
            "notes": obj.get("notes") if isinstance(obj.get("notes"), list) else []
        })

    return {"objects": normalized_objects, "warnings": warnings}


def _is_databricks_circuit_open() -> bool:
    return time.monotonic() < _databricks_circuit_open_until


def _record_databricks_success():
    global _databricks_failure_count, _databricks_circuit_open_until
    _databricks_failure_count = 0
    _databricks_circuit_open_until = 0.0


def _record_databricks_failure():
    global _databricks_failure_count, _databricks_circuit_open_until
    _databricks_failure_count += 1
    if _databricks_failure_count >= DATABRICKS_CIRCUIT_FAILURE_THRESHOLD:
        _databricks_circuit_open_until = time.monotonic() + DATABRICKS_CIRCUIT_OPEN_SECONDS
        print(f"[AI] Databricks circuit opened for {DATABRICKS_CIRCUIT_OPEN_SECONDS}s")


def _call_databricks_translation(system_prompt: str, input_ddl_json: dict) -> dict:
    if not DATABRICKS_LLM_INVOCATIONS_URL or not DATABRICKS_LLM_TOKEN:
        raise RuntimeError("Databricks LLM endpoint is not configured")

    headers = {
        "Authorization": f"Bearer {DATABRICKS_LLM_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Convert this schema:\n{json.dumps(input_ddl_json, indent=2)}"}
        ],
        "temperature": 0.1,
        "max_tokens": 8192
    }

    response = requests.post(
        DATABRICKS_LLM_INVOCATIONS_URL,
        headers=headers,
        json=payload,
        timeout=DATABRICKS_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    body = response.json()
    content = _extract_llm_content(body).strip()
    if not content:
        raise RuntimeError("Databricks LLM returned empty response")

    # Try strict JSON first.
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # If model wrapped JSON in code fences, strip and retry.
    if "```" in content:
        stripped = content.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed

    # If content is plain SQL, wrap into the expected translation shape.
    objects = input_ddl_json.get("objects") or [{}]
    first = objects[0] if objects else {}
    return {
        "objects": [
            {
                "name": first.get("name", "object"),
                "kind": first.get("kind", "table"),
                "schema": first.get("schema"),
                "target_sql": content,
                "notes": ["Translated by Databricks serving endpoint"]
            }
        ],
        "warnings": []
    }


def _translate_with_databricks_retry(system_prompt: str, input_ddl_json: dict) -> dict:
    if _is_databricks_circuit_open():
        raise RuntimeError("Databricks circuit open due to repeated failures")

    last_err = None
    for attempt in range(1, DATABRICKS_MAX_RETRIES + 1):
        try:
            result = _call_databricks_translation(system_prompt, input_ddl_json)
            default_obj = (input_ddl_json.get("objects") or [{}])[0]
            normalized = _normalize_translation_result(result, default_obj)
            _record_databricks_success()
            return normalized
        except Exception as e:
            last_err = e
            _record_databricks_failure()
            if attempt < DATABRICKS_MAX_RETRIES:
                delay = min(8, 2 ** (attempt - 1))
                print(f"[AI] Databricks attempt {attempt} failed; retrying in {delay}s: {e}")
                time.sleep(delay)
    raise RuntimeError(f"Databricks translation failed after {DATABRICKS_MAX_RETRIES} attempts: {last_err}")

async def translate_schema(source_dialect: str, target_dialect: str, input_ddl_json: dict) -> dict:
    print(f"[AI] Attempting translation from {source_dialect} to {target_dialect}")
    print(f"[AI] Model being used: {model}")
    print(f"[AI] Number of objects to translate: {len(input_ddl_json.get('objects', []))}")
    
    if "databricks" in (target_dialect or "").lower():
        system_prompt = """You are a database migration expert. Convert oracle DDL into Databricks SQL.

   interactive:
  sql:
    sql_script: 
      You are a database migration expert. Convert **Oracle SQL** into
      Databricks-compatible **Databricks SQL** (SQL-only).
      
      Output:
        - Return **Databricks SQL only**, each statement ends with a semicolon.
        - [MANDATORY] Do NOT wrap it in backticks, code fences, or a language tag.
        - [MANDATORY] NEVER use IDENTIFIER() function - always use backtick-quoted table names like `table_name`
        - [MANDATORY] Convert schema-qualified names like "schema"."table" to simple backtick names like `table`
      
      Key conversion considerations:
        - Remove or correct double quotes (`"Column"`)
        - [MANDATORY] NEVER use IDENTIFIER() function in output - this is for notebooks only
        - [MANDATORY] Convert schema-qualified table names to simple backtick names
        - [MANDATORY] If the source is NUMBER with no precision and no scale (e.g., NUMBER), always convert to INT.
        - [MANDATORY] If the source is NUMBER(p) with no scale (e.g., NUMBER(10)), always convert to DECIMAL(p).
        - [MANDATORY] If the source is NUMBER(p, s) with a scale (e.g., NUMBER(10,2)), always convert to DECIMAL(p, s).
        - [MANDATORY] Map Oracle NUMBER (with no precision and no scale) strictly to INT. Do NOT use DECIMAL for plain NUMBER columns.
        - Parameter markers (e.g., :param) are currently not allowed in the body of a CREATE VIEW statement in Databricks SQL. Do not Use parameters in CREATE VIEW. Use params in all other types of SQL.
        - Ensure all syntax is 100% compatible with the Databricks SQL engine on Databricks Runtime 14.x or newer.
        - Maintain the original logic, formatting, and comments from the source query.
        - Do not add any of your own commentary, explanations, or markdown formatting.
        - Return ONLY the raw, runnable Databricks SQL code.
        - DO NOT add ticks and `sql` keyword at the beginning and end of the conversions. Return just the converted SQL.
        - Replace variables with actual values in the procedure and instead of dynamic SQL Use regular queries. 
        - Only convert if the operation involves matching records between source and target tables
        - Do not convert simple single-table operations without joins or complex conditions
        - Change stored procedures to multi-line SQL statements, do not convert to a stored procedure.
      
      %%##conversion_prompts##%%
      %%##additional_prompts##%%
      
      --- START OF SQL ---
      {oracle_sql}
      --- END OF SQL ---

notebook:
  sql:
    sql_script: |
      You are a database migration expert. Convert Oracle SQL into a Databricks-compatible Databricks SQL notebook (SQL-only).
      
      Conversion rules:
      
      - Storage Clause Translation: Oracle-specific storage clauses (PARTITION BY RANGE/LIST/HASH, TABLESPACE, STORAGE) must be converted to Databricks SQL syntax.
      - Partitioning: Replace PARTITION BY RANGE (...) (...) blocks entirely. Extract the column name used in the range and apply it to a CLUSTER BY (column_name) clause at the end of the CREATE TABLE statement.
      - No Named Partitions: Remove all PARTITION <name> VALUES LESS THAN (...) syntax as it is not supported in Databricks. 
      - [MANDATORY] Try to not put special characters in variable names. If you have to include special characters in key, or include semicolon in value, please Use backquotes, e.g., SET `key`=`value`.
      - [MANDATORY] Table Storage: Every CREATE TABLE statement must explicitly include the USING DELTA clause before the closing semicolon. Do not change any other column definitions or logic while adding this clause
      - [MANDATORY] When converting to STRING or BINARY, remove any length specifications from the source. For example, RAW(16) must become BINARY, and UROWID(4000). For VARCHAR2(X) and NVARCHAR2(X), convert to VARCHAR(X) and preserve the length specification.
      - [MANDATORY] Do not include parentheses or length values for Databricks native types (STRING, BINARY, INT, BOOLEAN)  
      - [MANDATORY] ONLY if a column in a CREATE TABLE statement uses the DEFAULT keyword, you must append the following property at the very end of the statement (after USING DELTA): TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported'). If no DEFAULT constraints exist, do NOT add TBLPROPERTIES.
      - [MANDATORY] Ensure the DEFAULT <value> stays within the column definition logic, and only the TBLPROPERTIES is added at the end when DEFAULT constraints exist.
      - [MANDATORY] Convert all variations of Oracle Timestamps (TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE) simply to TIMESTAMP.
      - Forbidden: Do NOT include CHECK or UNIQUE keywords inside the CREATE TABLE (...) parentheses.
      - [MANDATORY] Standalone ALTER TABLE is ONLY for CHECK and UNIQUE. All PRIMARY KEY and FOREIGN KEY must stay inside the CREATE TABLE block.
      - MANDATORY REMOVAL: Identify all CHECK and UNIQUE constraints. You MUST remove them from the body of the CREATE TABLE statement entirely.
      - MANDATORY ALTER: Generate each CHECK and UNIQUE constraint ONLY as a standalone ALTER TABLE statement appearing after the CREATE TABLE statement.
      - KEY PRESERVATION: Keep PRIMARY KEY and FOREIGN KEY inside the CREATE TABLE definition. Do not move them to ALTER.
      - [MANDATORY] Default Value Property: If any column definition within the CREATE TABLE statement contains the keyword DEFAULT, you MUST append the TBLPROPERTIES clause immediately following USING DELTA.
      - [MANDATORY] Syntax: Always use CREATE TABLE (do NOT use OR REPLACE).
      - The syntax must be: USING DELTA TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
      - Convert all Oracle NCHAR(n) and NVARCHAR2(n) types to CHAR(n) and VARCHAR(n) respectively. Remove the N prefix but keep the fixed-length length logic. Convert VARCHAR2(n) to VARCHAR(n) and preserve the length specification.
      - Remove or correct double-quoted identifiers (e.g., "Column" → Column or `Column`).
      - Map types/functions to Databricks SQL: Maintain VARCHAR and CHAR types with their specified lengths (e.g., VARCHAR2(100) becomes VARCHAR(100), CHAR(10) remains CHAR(10)). Always preserve the length specifications in parentheses for VARCHAR and CHAR types.
      - Ensure syntax is valid on Databricks SQL (DBR 14.x+).
      - Preserve original logic and formatting. Comments are allowed, but keep them  concise and in proper SQL comment syntax.
      - Parameter markers (e.g., :param) are currently not allowed in the body of a CREATE VIEW statement in Databricks SQL. Do not Use parameters in CREATE VIEW. Use params in all other types of SQL.
      - Convert separate INSERT/UPDATE/DELETE operations into a single MERGE statement. Focus on: 1) Proper join conditions, 2) WHEN MATCHED/NOT MATCHED logic, 3) Error handling, and 4) Performance optimization through single table access. For example:
          MERGE INTO target USING source
          ON target.key = source.key
          WHEN MATCHED AND target.marked_for_deletion THEN DELETE
          WHEN MATCHED THEN UPDATE SET target.updated_at = source.updated_at, target.value = DEFAULT
      - [MANDATORY] UPDATE in Databricks does not support FROM another table. For updating values from one table into another, Use MERGE. Do NOT Use `UPDATE ... FROM ...` under any circumstance.
      - Analyze DDLs and identify all tables with ''fact'' in their name (case-insensitive). For each fact table found, change the CREATE TABLE statement to include CLUSTER BY AUTO for automatic liquid clustering optimization in Databricks. For example:
          CREATE OR REPLACE TABLE ... (
          id INT,
          name STRING,
          value DOUBLE
          )
          CLUSTER BY AUTO;
      
      If certain procedural parts cannot be fully expressed in SQL-only form, produce the best possible SQL-only approximation using sequential cells, TEMP VIEWs, MERGE/INSERT/COPY INTO, and deterministic set-based steps.
      
      %%##conversion_prompts##%%
      %%##additional_prompts##%%
      
      --- START OF SQL ---
      {oracle_sql}
      --- END OF SQL ---

    procedure: |
      You are a Databricks migration assistant. Your task is to convert Oracle SQL stored procedures into
      Databricks-compatible **Databricks SQL notebooks** (SQL-only).
    
      Requirements:
        - Focus only on the **core business logic** (tables, transformations, DML/DDL).
        - **Do not** include logging, audit checkpoints, or procedural status updates.
        - **Do not** include explanations, prose, or unnecessary comments.
        - If the procedure has procedural loops/branches that cannot be expressed in SQL, refactor them into set-based SQL or split into multiple sequential cells with deterministic steps.
      
      HARD REQUIREMENTS (DO NOT SKIP):
        - `IDENTIFIER` usage is not allowed with (temporary) VIEWs. Change [TEMP] or regular VIEWs to just TABLEs [NOT temp] to Use IDENTIFIER. Use target_schema for creating tables.
        -  Reference widgets as `:widget` or `IDENTIFIER(:widget || ''.table'')`. $widget usage is not allowed inside the stored procedure.
        - Try to not put special characters in variable names. If you have to include special characters in key, or include semicolon in value, please Use backquotes, e.g., SET `key`=`value`
      """
    else:
        system_prompt = f"""You are an expert database migration engine. Convert DDL from {source_dialect} to {target_dialect}.

CRITICAL RULES:
1. PRESERVE ALL COLUMNS - Do not drop or omit any columns from the source schema
2. Convert data types accurately:
   - INT64 → BIGINT
   - STRING → TEXT
   - FLOAT64 → DOUBLE PRECISION
   - NUMERIC → NUMERIC
   - TIMESTAMP → TIMESTAMP
   - DATE → DATE
   - BOOL → BOOLEAN
3. Keep column names EXACTLY as in source (case-sensitive)
4. Preserve all constraints, indexes, and relationships

Output strictly valid JSON:
{{
  "objects": [
    {{
      "name": "TableName",
      "kind": "table",
      "target_sql": "CREATE TABLE TableName (col1 TYPE, col2 TYPE, ...);",
      "notes": ["conversion notes"]
    }}
  ],
  "warnings": []
}}"""
    
    # 1) Primary path: Databricks serving endpoint (applies across all source dialects).
    try:
        print("[AI] Making API call to Databricks serving endpoint")
        result = await asyncio.to_thread(_translate_with_databricks_retry, system_prompt, input_ddl_json)
        print(f"[AI] Databricks translation successful with {len(result.get('objects', []))} objects")
        return result
    except Exception as databricks_error:
        print(f"[AI] Databricks translation error: {databricks_error}")

    # 2) Secondary path: existing OpenAI client if configured.
    if client and model:
        try:
            print("[AI] Making API call to OpenAI")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Convert this schema:\n{json.dumps(input_ddl_json, indent=2)}"}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            print("[AI] API call successful")

            result = json.loads(response.choices[0].message.content)
            print(f"[AI] Parsed result with {len(result.get('objects', []))} objects")
            return result
        except Exception as e:
            print(f"[AI] OpenAI translation error: {str(e)}")

    # 3) Final path: explicit error marker so caller can fallback_translation.
    return {
        "objects": [],
        "warnings": ["LLM translation unavailable. Using fallback translation."],
        "error": "Databricks and OpenAI translation paths failed"
    }

async def suggest_fixes(validation_failures_json: dict) -> dict:
    if not client or not model:
        return {
            "fixes": [],
            "error": "OpenAI client not available"
        }
    
    system_prompt = "You are an expert DB reliability engineer. Given validation failures, propose precise fixes (SQL or config) with brief rationale. Return JSON with format: { 'fixes': [ { 'category': str, 'issue': str, 'fix': str, 'rationale': str, 'confidence': float } ] }"
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Suggest fixes for:\n{json.dumps(validation_failures_json, indent=2)}"}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        return {
            "fixes": [],
            "error": str(e)
        }

def fallback_translation(objects_list: list, source_dialect: str, target_dialect: str) -> dict:
    """
    Basic fallback translation that converts MySQL DDL to PostgreSQL DDL.
    Performs simple string replacements for common data types and syntax.
    """
    print(f"[FALLBACK] Translation from {source_dialect} to {target_dialect}")
    print(f"[FALLBACK] Received {len(objects_list)} objects")
    
    translated_objects = []
    
    for obj in objects_list:
        obj_name = obj.get("name", "unknown")
        obj_kind = obj.get("kind", "table")
        obj_schema = obj.get("schema") or obj.get("table_schema") or "public"
        source_ddl = obj.get("source_ddl", "")
        
        print(f"[FALLBACK] Processing {obj_name}, source_ddl length: {len(source_ddl)}")
        print(f"[FALLBACK] First 150 chars of source DDL: {source_ddl[:150]}")
        
        import re
        
        if not source_ddl:
            # No source DDL available
            target_ddl = f'CREATE TABLE "{obj_name}" (\n  id SERIAL PRIMARY KEY\n);'
            notes = [
                f"No source DDL available for {obj_name}",
                "Created simplified table structure"
            ]
        elif ("bigquery" in source_dialect.lower()) and ("postgresql" in target_dialect.lower()):
            # Convert BigQuery DDL to PostgreSQL
            print(f"[FALLBACK] Applying BigQuery -> PostgreSQL conversion")
            target_ddl = source_ddl
            
            # Replace backticks with double quotes (BigQuery style to PostgreSQL style)
            target_ddl = target_ddl.replace('`', '"')
            
            # Remove schema prefix from table name (e.g., "schema.table" -> "table")
            target_ddl = re.sub(r'CREATE\s+TABLE\s+"[^"]+\.([^"]+)"', r'CREATE TABLE "\1"', target_ddl, flags=re.IGNORECASE)
            
            # Type conversions for BigQuery -> PostgreSQL
            target_ddl = re.sub(r'\bINT64\b', 'BIGINT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bINTEGER\b', 'INTEGER', target_ddl, flags=re.IGNORECASE)  # Keep INTEGER as-is
            target_ddl = re.sub(r'\bFLOAT64\b', 'DOUBLE PRECISION', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bFLOAT\b', 'REAL', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bSTRING\b', 'TEXT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBOOL\b', 'BOOLEAN', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBYTES\b', 'BYTEA', target_ddl, flags=re.IGNORECASE)
            
            # Remove BigQuery-specific OPTIONS clause
            target_ddl = re.sub(r'\s*OPTIONS\s*\([^)]*\)', '', target_ddl, flags=re.IGNORECASE)
            
            print(f"[FALLBACK] Converted to: {target_ddl[:150]}")
            
            notes = [
                f"Fallback translation from BigQuery to PostgreSQL",
                "Basic type conversions applied"
            ]
        elif ("bigquery" in source_dialect.lower()) and ("mysql" in target_dialect.lower()):
            # Convert BigQuery DDL to MySQL
            print(f"[FALLBACK] Applying BigQuery -> MySQL conversion")
            target_ddl = source_ddl
            
            # Replace backticks (BigQuery already uses backticks, keep them for MySQL)
            # But remove schema prefix from table name (e.g., `schema.table` -> `table`)
            target_ddl = re.sub(r'CREATE\s+TABLE\s+`[^`]+\.([^`]+)`', r'CREATE TABLE `\1`', target_ddl, flags=re.IGNORECASE)
            
            # Type conversions for BigQuery -> MySQL
            target_ddl = re.sub(r'\bINT64\b', 'BIGINT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bINTEGER\b', 'INT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bFLOAT64\b', 'DOUBLE', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bFLOAT\b', 'FLOAT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bSTRING\b', 'TEXT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBOOL\b', 'TINYINT(1)', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBYTES\b', 'BLOB', target_ddl, flags=re.IGNORECASE)
            
            # Remove BigQuery-specific OPTIONS clause
            target_ddl = re.sub(r'\s*OPTIONS\s*\([^)]*\)', '', target_ddl, flags=re.IGNORECASE)
            
            print(f"[FALLBACK] Converted to: {target_ddl[:150]}")
            
            notes = [
                f"Fallback translation from BigQuery to MySQL",
                "Basic type conversions applied"
            ]
        elif ("mysql" in source_dialect.lower()) and ("postgresql" in target_dialect.lower()):
            # Convert MySQL DDL to PostgreSQL DDL
            target_ddl = source_ddl
            
            # Replace backticks with double quotes
            target_ddl = target_ddl.replace('`', '"')
            
            # Convert AUTO_INCREMENT to SERIAL for integer primary key columns
            # Pattern: "column_name" int NOT NULL AUTO_INCREMENT
            target_ddl = re.sub(
                r'"(\w+)"\s+int\s+NOT\s+NULL\s+AUTO_INCREMENT',
                r'"\1" SERIAL',
                target_ddl,
                flags=re.IGNORECASE
            )
            
            # Convert remaining int to INTEGER
            target_ddl = re.sub(r'\bint\b', 'INTEGER', target_ddl, flags=re.IGNORECASE)
            
            # Remove ENGINE, AUTO_INCREMENT value, and CHARSET clauses
            target_ddl = re.sub(r'\s*ENGINE\s*=\s*\w+', '', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\s*AUTO_INCREMENT\s*=\s*\d+', '', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\s*DEFAULT\s+CHARSET\s*=\s*\w+', '', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\s*CHARSET\s*=\s*\w+', '', target_ddl, flags=re.IGNORECASE)
            
            # Clean up extra semicolons
            target_ddl = target_ddl.strip().rstrip(';') + ';'
            
            notes = [
                f"Basic fallback translation from {source_dialect} to {target_dialect}",
                "Performed simple syntax conversions",
                "AI engine provides more accurate translations with complex types"
            ]
        elif ("mysql" in source_dialect.lower()) and ("snowflake" in target_dialect.lower()):
            # Convert MySQL DDL to Snowflake DDL (minimal, selection-driven migration support)
            target_ddl = source_ddl

            # MySQL uses backticks; Snowflake accepts double quotes for identifiers.
            target_ddl = target_ddl.replace("`", '"')

            # Strip schema/database qualifiers from CREATE TABLE target (Snowflake schema is fixed via connection context).
            m = re.match(r'(?is)^\s*(CREATE\s+TABLE\s+)(IF\s+NOT\s+EXISTS\s+)?(?P<name>[^(\n]+)\s*\(', target_ddl)
            if m:
                raw_name = (m.group("name") or "").strip()
                parts = [p.strip() for p in raw_name.split(".") if p.strip()]
                table_only = parts[-1] if parts else raw_name
                table_only = table_only.strip().strip('"')
                prefix = m.group(1)
                if_part = m.group(2) or ""
                target_ddl = re.sub(
                    r'(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^(\n]+\s*\(',
                    f'{prefix}{if_part}"{table_only}" (',
                    target_ddl,
                    count=1
                )

            # Remove MySQL-only table options and unsupported attributes
            target_ddl = re.sub(r'\bUNSIGNED\b', '', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bZEROFILL\b', '', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bAUTO_INCREMENT\b', 'AUTOINCREMENT', target_ddl, flags=re.IGNORECASE)

            # Remove index definitions from SHOW CREATE TABLE (Snowflake doesn't support secondary indexes)
            target_ddl = re.sub(
                r',\s*(?:UNIQUE\s+)?(?:FULLTEXT\s+|SPATIAL\s+)?KEY\s+"?[^"\s(]+"?\s*\([^)]*\)\s*(?:USING\s+\w+)?',
                '',
                target_ddl,
                flags=re.IGNORECASE
            )

            # Remove ENGINE / CHARSET / COLLATE / COMMENT / ROW_FORMAT / AUTO_INCREMENT= table options at the end
            target_ddl = re.sub(r'\)\s*ENGINE\s*=\s*[^;]+', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*DEFAULT\s+CHARSET\s*=\s*[^;]+', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*CHARSET\s*=\s*[^;]+', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*COLLATE\s*=\s*[^;]+', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*ROW_FORMAT\s*=\s*[^;]+', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*COMMENT\s*=\s*\'[^\']*\'', ')', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\)\s*AUTO_INCREMENT\s*=\s*\d+', ')', target_ddl, flags=re.IGNORECASE)

            # Clean up whitespace and ensure a single trailing semicolon
            target_ddl = re.sub(r'\s+\)', ')', target_ddl)
            target_ddl = target_ddl.strip().rstrip(';') + ';'

            notes = [
                f"Basic fallback translation from {source_dialect} to {target_dialect}",
                "Removed MySQL-only table options",
                "Dropped secondary indexes (Snowflake unsupported)",
                "Target schema is controlled by Snowflake connection context"
            ]
        elif ("oracle" in source_dialect.lower()) and ("databricks" in target_dialect.lower()):
            # Convert Oracle DDL to Databricks SQL (basic, table-only).
            target_ddl = source_ddl or f'CREATE TABLE "{obj_name}" (id NUMBER);'

            # Remove schema qualifier from CREATE TABLE and switch to backticks.
            target_ddl = re.sub(
                r'(?is)^\s*CREATE\s+TABLE\s+"[^"]+"\."([^"]+)"\s*\(',
                r'CREATE TABLE `\1` (',
                target_ddl,
                count=1
            )

            # Normalize identifiers to backticks for Databricks.
            target_ddl = target_ddl.replace('"', '`')

            # Type conversions (Oracle -> Databricks).
            target_ddl = re.sub(r'\bVARCHAR2\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bNVARCHAR2\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bNCHAR\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bCHAR\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bCLOB\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bNCLOB\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBLOB\b', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bRAW\b', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBINARY_FLOAT\b', 'FLOAT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBINARY_DOUBLE\b', 'DOUBLE', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bFLOAT\b', 'DOUBLE', target_ddl, flags=re.IGNORECASE)

            # NUMBER(p,s) -> DECIMAL(p,s), NUMBER(p) -> DECIMAL(p,0), NUMBER -> DECIMAL(38,10)
            target_ddl = re.sub(
                r'\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)',
                r'DECIMAL(\1,\2)',
                target_ddl,
                flags=re.IGNORECASE
            )
            target_ddl = re.sub(
                r'\bNUMBER\s*\(\s*(\d+)\s*\)',
                r'DECIMAL(\1,0)',
                target_ddl,
                flags=re.IGNORECASE
            )
            target_ddl = re.sub(r'\bNUMBER\b', 'DECIMAL(38,10)', target_ddl, flags=re.IGNORECASE)

            # Date/time defaults.
            target_ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', target_ddl, flags=re.IGNORECASE)

            # Clean up trailing semicolons.
            target_ddl = target_ddl.strip().rstrip(';') + ';'

            notes = [
                f"Fallback translation from {source_dialect} to {target_dialect}",
                "Converted common Oracle types to Databricks types",
                "Dropped schema qualifier to use Databricks connection schema"
            ]
        elif ("postgres" in source_dialect.lower()) and ("snowflake" in target_dialect.lower()):
            # Convert PostgreSQL DDL to Snowflake DDL (minimal, focusing on common Postgres features).
            # This is intentionally conservative: it aims to produce runnable DDL for typical tables,
            # sequences, and defaults (e.g., nextval(...) and UUID generators).
            target_ddl = source_ddl

            # Normalize common Postgres casts that show up in defaults.
            target_ddl = re.sub(r"::\s*regclass\b", "", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"::\s*text\b", "", target_ddl, flags=re.IGNORECASE)

            # Type conversions (common).
            target_ddl = re.sub(r"\bBIGSERIAL\b", "BIGINT AUTOINCREMENT", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bSERIAL\b", "INTEGER AUTOINCREMENT", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bcharacter\s+varying\b", "VARCHAR", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bdouble\s+precision\b", "DOUBLE", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bUUID\b", "VARCHAR(36)", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bJSONB\b", "VARIANT", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bJSON\b", "VARIANT", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bBYTEA\b", "BINARY", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b", "TIMESTAMP_TZ", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bTIMESTAMP\s+WITHOUT\s+TIME\s+ZONE\b", "TIMESTAMP_NTZ", target_ddl, flags=re.IGNORECASE)

            # Default conversions.
            target_ddl = re.sub(r"\bgen_random_uuid\s*\(\s*\)", "UUID_STRING()", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\buuid_generate_v4\s*\(\s*\)", "UUID_STRING()", target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r"\bnow\s*\(\s*\)", "CURRENT_TIMESTAMP()", target_ddl, flags=re.IGNORECASE)

            # Postgres nextval('schema.seq') => schema.seq.NEXTVAL (Snowflake sequence usage).
            def _nextval_repl(m):
                raw = (m.group(1) or m.group(2) or m.group(3) or "").strip()
                raw = raw.strip("'").strip('"').replace('"', "")
                raw = re.sub(r"\\s+", "", raw)
                return f"{raw}.NEXTVAL" if raw else "NULL"

            target_ddl = re.sub(
                r"(?i)\bnextval\s*\(\s*(?:'([^']+)'|\"([^\"]+)\"|([^\)]+))\s*\)",
                _nextval_repl,
                target_ddl,
            )

            # Sequence DDL differences.
            if obj_kind == "sequence":
                start_m = re.search(r"(?im)\bSTART\s+WITH\s+(\d+)\b", source_ddl or "")
                inc_m = re.search(r"(?im)\bINCREMENT\s+BY\s+(-?\d+)\b", source_ddl or "")
                start_v = start_m.group(1) if start_m else None
                inc_v = inc_m.group(1) if inc_m else None

                qualified = f"\"{obj_schema}\".\"{obj_name}\"" if obj_schema else f"\"{obj_name}\""
                opts = []
                if start_v is not None:
                    opts.append(f"START = {start_v}")
                if inc_v is not None:
                    opts.append(f"INCREMENT = {inc_v}")

                target_ddl = f"CREATE SEQUENCE IF NOT EXISTS {qualified}"
                if opts:
                    target_ddl += " " + " ".join(opts)
                target_ddl = target_ddl.strip().rstrip(";") + ";"

            notes = [
                f"Fallback translation from {source_dialect} to {target_dialect}",
                "Converted SERIAL/BIGSERIAL, UUID, JSON/JSONB, timestamp types",
                "Rewrote nextval(...) defaults to <sequence>.NEXTVAL where possible",
            ]
        else:
            # Unsupported translation path - use source DDL as-is
            target_ddl = source_ddl if source_ddl else f'CREATE TABLE "{obj_name}" (id SERIAL PRIMARY KEY);'
            notes = [
                f"Unsupported translation from {source_dialect} to {target_dialect}",
                "Using source DDL as-is or creating simplified table"
            ]
        
        translated_objects.append({
            "name": obj_name,
            "kind": obj_kind,
            "schema": obj_schema,
            "target_sql": target_ddl,
            "notes": notes
        })
    
    return {
        "objects": translated_objects,
        "warnings": [
            "Using fallback translation engine - AI translation unavailable",
            "Fallback performs basic syntax conversions only",
            "Please review DDL before production deployment"
        ]
    }
