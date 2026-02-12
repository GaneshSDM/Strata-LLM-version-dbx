#ai.py
import os
import json
import asyncio
import time
from pathlib import Path
from dotenv import load_dotenv
import requests
from cryptography.fernet import Fernet
try:
    # Prefer package-relative import when used as part of the backend package
    from .encryption import get_fernet_key
except Exception:
    # Fallback for direct execution contexts
    try:
        from backend.encryption import get_fernet_key  # type: ignore
    except ImportError:
        # Last resort: try direct import from current directory
        import sys
        import os
        current_dir = os.path.dirname(__file__)
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        from encryption import get_fernet_key

# Load env vars from both project root .env and backend/.env.
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
load_dotenv(Path(__file__).resolve().parent / ".env")

print("[AI MODULE] Loading AI module")
# Note: OpenAI integration removed per request. Only Databricks LLM will be used.

# Databricks serving endpoint support (used as primary translator when configured).
DATABRICKS_LLM_INVOCATIONS_URL = os.environ.get(
    "DATABRICKS_LLM_INVOCATIONS_URL",
    "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations"
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

print("[AI MODULE] Using Databricks LLM exclusively.")


def _extract_llm_content(result_json: dict) -> str:
    # Try OpenAI-style format first
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
    
    # Try Databricks predictions format
    predictions = result_json.get("predictions") or []
    if predictions and isinstance(predictions, list):
        pred = predictions[0]
        if isinstance(pred, dict):
            content = pred.get("content") or pred.get("text") or pred.get("generated_text")
            if isinstance(content, str):
                return content
        elif isinstance(pred, str):
            return pred
    
    # Fallback for raw text payloads
    if isinstance(result_json.get("text"), str):
        return result_json["text"]
    
    # Last resort: try to find any string field that might contain the content
    for key in ["content", "response", "output", "result"]:
        if isinstance(result_json.get(key), str):
            return result_json[key]
    
    return ""


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
    
    # Extract source_ddl from the input JSON
    objects = input_ddl_json.get("objects") or [{}]
    first = objects[0] if objects else {}
    source_ddl = first.get("source_ddl", "")
    
    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Convert this Oracle DDL to Databricks SQL:\n{source_ddl}"}
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
    
    # DEBUG: Log the raw response structure
    print(f"[AI DEBUG] Raw response keys: {body.keys()}")
    if isinstance(body.get("choices"), list) and len(body.get("choices")) > 0:
        print(f"[AI DEBUG] First choice keys: {body['choices'][0].keys() if isinstance(body['choices'][0], dict) else 'not dict'}")
    
    # Extract content from response
    content = _extract_llm_content(body).strip()
    print(f"[AI DEBUG] Extracted content length: {len(content)}")
    if not content:
        # Try alternative extraction for Databricks-specific formats
        if "predictions" in body:
            predictions = body["predictions"]
            if isinstance(predictions, list) and len(predictions) > 0:
                pred = predictions[0]
                if isinstance(pred, dict):
                    content = pred.get("content", "") or pred.get("text", "") or pred.get("generated_text", "")
                    print(f"[AI DEBUG] Extracted from predictions: {len(content)} chars")
                elif isinstance(pred, str):
                    content = pred
                    print(f"[AI DEBUG] Extracted from predictions (string): {len(content)} chars")
        if not content:
            raise RuntimeError("Databricks LLM returned empty response")

    # Strip code fences if present (basic cleanup)
    if content.startswith("```"):
        lines = content.split('\n')
        if len(lines) >= 3:
            # Remove first ```json or ``` line and last ``` line
            content = '\n'.join(lines[1:-1])
            if content.endswith('```'):
                content = content[:-3].strip()
    
    # Use the raw SQL content directly as target_sql (no JSON parsing needed)
    target_sql = content.strip()
    print(f"[AI] Translated SQL length: {len(target_sql)} chars")
    
    objects = input_ddl_json.get("objects") or [{}]
    first = objects[0] if objects else {}
    return {
        "objects": [
            {
                "name": first.get("name", "object"),
                "kind": first.get("kind", "table"),
                "schema": first.get("schema"),
                "target_sql": target_sql,
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
            _record_databricks_success()
            return result  # Return raw result from LLM - no normalization
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
    print(f"[AI] Number of objects to translate: {len(input_ddl_json.get('objects', []))}")
    
    if "databricks" in (target_dialect or "").lower():
        system_prompt = """You are a database migration expert specializing in Oracle to Databricks SQL conversion.
TASK
Convert Oracle SQL / Oracle DDL into Databricks-compatible Databricks SQL (DBR 14.x+).

CRITICAL OUTPUT RULES
- Return Databricks SQL only inside `target_sql`.
- Every statement MUST end with a semicolon.
- [MANDATORY] Do NOT wrap target_sql in backticks, code fences, or language tags.
- Do NOT include explanations, prose, or markdown formatting in `target_sql`.
- Return ONLY raw, runnable Databricks SQL code in target_sql.
- Keep column names EXACTLY as in source (case-sensitive)
- Remove Oracle-specific syntax: ENABLE, USING INDEX, USING INDEX ENABLE, PCTFREE, NOCOMPRESS, TABLESPACE, SEGMENT CREATION DEFERRED, STORAGE, PARTITION

COLUMN NAME QUOTING RULE (STRICT)
 
- Column names MUST NOT be wrapped in single quotes.
- Column names MUST NOT be wrapped in double quotes.
- Column names MUST NOT be wrapped in backticks.
- Output column names as plain identifiers only.
 
Correct:
    EMP_ID
    DEPARTMENT_NAME
 
Incorrect:
    "EMP_ID"
    'EMP_ID'
    `EMP_ID`
 
If the source contains quoted column names, remove the quotes

HARD NORMALIZATION (MANDATORY):
- The keywords ENABLE, DISABLE, and USING INDEX are FORBIDDEN in output.
- If PRIMARY KEY appears, it must be emitted WITHOUT USING INDEX or ENABLE.
- Replace:
  - "NOT NULL ENABLE" → "NOT NULL"
  - "PRIMARY KEY (...) USING INDEX ENABLE" → "PRIMARY KEY (...)"
- After generating target_sql, perform a final cleanup pass:
  - Remove any remaining occurrences of ENABLE or USING INDEX.
 
NOT NULL & ENABLE HANDLING (CRITICAL)
- If a column contains NOT NULL ENABLE, output ONLY NOT NULL.
- The keyword ENABLE must never appear anywhere in the converted output.
- ENABLE must be treated as an Oracle enforcement detail and discarded completely.
 
Example rule:
- Oracle: COLUMN_A NUMBER NOT NULL ENABLE
-Databricks: COLUMN_A INT NOT NULL
 
IDENTIFIER HANDLING (MANDATORY)
- Remove or correct double-quoted identifiers ("Column" -> `Column` or Column).
- [MANDATORY] NEVER use IDENTIFIER() function in output - use backtick-quoted table names like `table_name`.
- [MANDATORY] Convert schema-qualified names like "schema"."table" to simple backtick names like `table`.
- Prefer backticks only when needed (reserved keywords, special characters).
 
DATA TYPE MAPPING (MANDATORY)
Oracle NUMBER conversions:
- [MANDATORY] NUMBER (no precision, no scale) -> INT (NOT DECIMAL)
- [MANDATORY] NUMBER(p) with no scale -> DECIMAL(p)
- [MANDATORY] NUMBER(p, s) with scale -> DECIMAL(p, s)
 
Oracle character types:
- VARCHAR2(n) -> VARCHAR(n) [preserve length]
- NVARCHAR2(n) -> VARCHAR(n) [preserve length, remove N prefix]
- CHAR(n) -> CHAR(n) [preserve length]
- NCHAR(n) -> CHAR(n) [preserve length, remove N prefix]
 
Oracle LOB types:
- CLOB -> STRING [no length specification]
- NCLOB -> STRING [no length specification]
- TEXT -> STRING [no length specification]
- BLOB -> BINARY [no length specification]
- RAW -> BINARY [no length specification]
 
Oracle date/time types:
- [MANDATORY] DATE -> TIMESTAMP
- [MANDATORY] TIMESTAMP -> TIMESTAMP (all variants: WITH TIME ZONE, WITH LOCAL TIME ZONE)
- DATE with DEFAULT CURRENT_TIMESTAMP -> DATE with DEFAULT CURRENT_DATE
- SYSDATE -> CURRENT_TIMESTAMP
 
Floating point:
- BINARY_FLOAT -> FLOAT
- BINARY_DOUBLE -> DOUBLE
- FLOAT -> DOUBLE
 
[MANDATORY] Do not include parentheses or length values for Databricks native types (STRING, BINARY, INT, BOOLEAN).
 
TABLE STORAGE (MANDATORY)
- [MANDATORY] Every CREATE TABLE statement must explicitly include USING DELTA before the closing semicolon.
- [MANDATORY] Syntax: Always use CREATE TABLE IF NOT EXISTS (not OR REPLACE).
- [MANDATORY] ONLY if any column uses the DEFAULT keyword, append TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') after USING DELTA.
- If no DEFAULT constraints exist, do NOT add TBLPROPERTIES.
- Ensure DEFAULT <value> stays within the column definition; only TBLPROPERTIES is added at the end.
 
ORACLE STORAGE CLAUSES (MANDATORY REMOVAL)
Remove these Oracle-specific clauses entirely:
- ENABLE
- USING INDEX
- TABLESPACE
- PCTFREE, INITRANS, MAXTRANS
- STORAGE (...)
- PARTITION <name> VALUES LESS THAN (...)
 
PARTITIONING
- Replace PARTITION BY RANGE/LIST/HASH blocks entirely.
- Extract the partition column and use CLUSTER BY (column_name) instead.
- Remove all named partition definitions.
 
CONSTRAINTS (CRITICAL)
- PRIMARY KEY and FOREIGN KEY:
- Must remain inside CREATE TABLE
- Never move to ALTER TABLE
CHECK constraints:
- Must be removed from CREATE TABLE
- Emit as separate ALTER TABLE ADD CONSTRAINT
UNIQUE constraints:
- Must be completely removed
- Do NOT emit ALTER TABLE for UNIQUE
- Do NOT infer or invent constraints.
 
NORMALIZATION EXAMPLES (MANDATORY):
 
Oracle:
PRIMARY KEY (EMP_ID) USING INDEX ENABLE
 
Databricks:
PRIMARY KEY (EMP_ID)
 
Oracle:
DEPARTMENT_ID NUMBER NOT NULL ENABLE
 
Databricks:
DEPARTMENT_ID DECIMAL(4,0) NOT NULL
 
FACT TABLE OPTIMIZATION
- If table name contains 'fact' (case-insensitive), append CLUSTER BY AUTO.
 
GENERAL QUALITY
- Maintain original logic, formatting, and comments from source.
- Ensure 100% compatibility with Databricks SQL engine on DBR 14.x or newer.
- Keep comments concise and in proper SQL syntax.

CORRECT OUTPUT EXAMPLE:
CREATE TABLE SAMPLE_CONSTRAINTS (EMP_ID DECIMAL(10), EMP_EMAIL VARCHAR(150)) USING DELTA;
ALTER TABLE SAMPLE_CONSTRAINTS ADD CONSTRAINT ch_email CHECK (EMP_EMAIL);

WRONG OUTPUT EXAMPLE:
CREATE TABLE `SAMPLE_CONSTRAINTS` (`EMP_ID` DECIMAL(10), `EMP_EMAIL` VARCHAR(150), UNIQUE (`EMP_EMAIL`)) USING DELTA;"""
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
   - DATE → TIMESTAMP
   - BOOL → BOOLEAN
3. Keep column names EXACTLY as in source (case-sensitive)
4. CONSTRAINT HANDLING (MANDATORY):
   - PRIMARY KEY and FOREIGN KEY constraints MUST remain in CREATE TABLE definition
   - CHECK constraints MUST be moved to separate ALTER TABLE ADD CONSTRAINT statements
   - UNIQUE constraints MUST be completely removed from output (do not create any ALTER TABLE statements for UNIQUE constraints) as Databricks Delta tables do not enforce them
   - Remove Oracle-specific syntax: ENABLE, USING INDEX, USING INDEX ENABLE
   - For Databricks: Use backticks for identifiers, add USING DELTA
5. Preserve all constraints, indexes, and relationships

Output strictly valid JSON:
{{
  "objects": [
    {{
      "name": "TableName",
      "kind": "table",
      "target_sql": "CREATE TABLE `TableName` (col1 TYPE, col2 TYPE, ...) USING DELTA;",
      "notes": ["conversion notes"]
    }}
  ],
  "warnings": []
}}"""
    
    # Databricks serving endpoint (applies across all source dialects).
    try:
        print("[AI] Making API call to Databricks serving endpoint")
        result = await asyncio.to_thread(_translate_with_databricks_retry, system_prompt, input_ddl_json)
        print(f"[AI] Databricks translation successful with {len(result.get('objects', []))} objects")
        return result
    except Exception as databricks_error:
        print(f"[AI] Databricks translation error: {databricks_error}")
        return {
            "objects": [],
            "warnings": [],
            "error": f"Translation failed: Databricks endpoint unavailable. {str(databricks_error)}"
        }


async def suggest_fixes(validation_failures_json: dict) -> dict:
    # OpenAI integration is disabled; Databricks LLM is the primary translation engine.
    return {
        "fixes": [],
        "error": "ENDPOINT ERROR"
    }
