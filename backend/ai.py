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

client = None  # No OpenAI client; Databricks LLM will be used exclusively.
print("[AI MODULE] OpenAI integration disabled – using Databricks LLM only.")


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
    print(f"[AI] Number of objects to translate: {len(input_ddl_json.get('objects', []))}")
    
    if "databricks" in (target_dialect or "").lower():
        # NOTE: This prompt is intentionally derived from the user's enterprise DDL conversion rubric.
        # We adapt it to this backend by:
        #   - taking input as JSON objects (each with source_ddl)
        #   - requiring output as JSON (objects[].target_sql) so the UI can display it reliably
        system_prompt = """You are a database migration expert specializing in Oracle to Databricks SQL conversion.

TASK
Convert Oracle SQL / Oracle DDL into Databricks-compatible Databricks SQL (DBR 14.x+).
The user input will be a JSON object with an `objects` array, where each object contains `source_ddl`.

MANDATORY OUTPUT FORMAT
Return STRICT JSON ONLY (no markdown, no code fences, no backticks wrapping the whole response):
{
  "objects": [
    {
      "name": "<string>",
      "kind": "table|view|sequence|procedure|function|other",
      "schema": "<string|null>",
      "target_sql": "<Databricks SQL text>",
      "notes": ["<short notes>"]
    }
  ],
  "warnings": ["<warning>"]
}

CRITICAL OUTPUT RULES
- Return Databricks SQL only inside `target_sql`. Every statement MUST end with a semicolon.
- [MANDATORY] Do NOT wrap target_sql in backticks, code fences, or language tags.
- Do NOT include explanations, prose, or markdown formatting in `target_sql`.
- Return ONLY raw, runnable Databricks SQL code in target_sql.

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

CONSTRAINTS (MANDATORY - CRITICAL FOR FK HANDLING)
- [MANDATORY] PRIMARY KEY and FOREIGN KEY MUST remain inside CREATE TABLE definition.
- [MANDATORY] Do NOT move PRIMARY KEY or FOREIGN KEY to ALTER TABLE statements.
- [MANDATORY] CHECK and UNIQUE constraints MUST be removed from CREATE TABLE body.
- [MANDATORY] Emit CHECK and UNIQUE as standalone ALTER TABLE statements after CREATE TABLE.
- The execution layer handles FK constraints properly (two-phase creation for self-referencing FKs).

FACT TABLE OPTIMIZATION
- If table name contains 'fact' (case-insensitive), append CLUSTER BY AUTO.

VIEWS
- [MANDATORY] Parameter markers (e.g., :param) are not allowed in CREATE VIEW body.
- Do not use parameters in CREATE VIEW. Use params in all other SQL types.

DML / MERGE (MANDATORY)
- [MANDATORY] UPDATE ... FROM is NOT supported in Databricks.
- For updating from another table, ALWAYS use MERGE INTO ... USING ... WHEN MATCHED.
- Convert separate INSERT/UPDATE/DELETE to a single MERGE statement when possible.
- Focus on: proper join conditions, WHEN MATCHED/NOT MATCHED logic, error handling.

PROCEDURES
- Convert stored procedures to multi-line SQL statements.
- Do NOT convert to stored procedure format.
- Replace variables with actual values.
- Convert dynamic SQL to regular queries.

GENERAL QUALITY
- Maintain original logic, formatting, and comments from source.
- Ensure 100% compatibility with Databricks SQL engine on DBR 14.x or newer.
- Keep comments concise and in proper SQL syntax.
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
    
    # 1) Primary path: Databricks serving endpoint (applies across all source dialects).
    try:
        print("[AI] Making API call to Databricks serving endpoint")
        result = await asyncio.to_thread(_translate_with_databricks_retry, system_prompt, input_ddl_json)
        print(f"[AI] Databricks translation successful with {len(result.get('objects', []))} objects")
        return result
    except Exception as databricks_error:
        print(f"[AI] Databricks translation error: {databricks_error}")

    # 2) Secondary path: existing OpenAI client if configured.
    if client:
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
    if not client:
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
            # Convert Oracle DDL to Databricks SQL (basic, table-focused).
            # This fallback aligns with the stricter enterprise DDL rules:
            #   - NUMBER -> INT (no p/s)
            #   - NUMBER(p) -> DECIMAL(p)
            #   - NUMBER(p,s) -> DECIMAL(p,s)
            #   - VARCHAR2/NVARCHAR2 -> VARCHAR(n)
            #   - NCHAR -> CHAR(n)
            #   - RAW/BLOB -> BINARY (no length)
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

            # Preserve VARCHAR/CHAR lengths; remove N prefix.
            target_ddl = re.sub(r'\bNVARCHAR2\s*\(', 'VARCHAR(', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bVARCHAR2\s*\(', 'VARCHAR(', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bNCHAR\s*\(', 'CHAR(', target_ddl, flags=re.IGNORECASE)

            # Large objects.
            target_ddl = re.sub(r'\bCLOB\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bNCLOB\b', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBLOB\b', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bRAW\s*\(\s*\d+\s*\)', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bRAW\b', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bUROWID\s*\(\s*\d+\s*\)', 'STRING', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bUROWID\b', 'STRING', target_ddl, flags=re.IGNORECASE)

            # Timestamp variants.
            target_ddl = re.sub(r'\bTIMESTAMP\s+WITH\s+LOCAL\s+TIME\s+ZONE\b', 'TIMESTAMP', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b', 'TIMESTAMP', target_ddl, flags=re.IGNORECASE)

            # Floating point.
            target_ddl = re.sub(r'\bBINARY_FLOAT\b', 'FLOAT', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bBINARY_DOUBLE\b', 'DOUBLE', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bFLOAT\b', 'DOUBLE', target_ddl, flags=re.IGNORECASE)

            # NUMBER rules.
            target_ddl = re.sub(
                r'\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)',
                r'DECIMAL(\1,\2)',
                target_ddl,
                flags=re.IGNORECASE
            )
            target_ddl = re.sub(
                r'\bNUMBER\s*\(\s*(\d+)\s*\)',
                r'DECIMAL(\1)',
                target_ddl,
                flags=re.IGNORECASE
            )
            target_ddl = re.sub(r'\bNUMBER\b', 'INT', target_ddl, flags=re.IGNORECASE)

            # Date/time defaults: SYSDATE is a TIMESTAMP in Oracle; for DATE defaults, prefer CURRENT_DATE.
            target_ddl = re.sub(r'\bDATE\s+DEFAULT\s+SYSDATE\b', 'DATE DEFAULT CURRENT_DATE', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', target_ddl, flags=re.IGNORECASE)

            # Remove any illegal type lengths for BINARY/STRING.
            target_ddl = re.sub(r'\bBINARY\s*\(\s*\d+\s*\)', 'BINARY', target_ddl, flags=re.IGNORECASE)
            target_ddl = re.sub(r'\bSTRING\s*\(\s*\d+\s*\)', 'STRING', target_ddl, flags=re.IGNORECASE)

            # Best-effort ensure USING DELTA for CREATE TABLE.
            if re.search(r'(?is)^\s*CREATE\s+TABLE\b', target_ddl) and not re.search(r'(?is)\bUSING\s+DELTA\b', target_ddl):
                # Insert USING DELTA before TBLPROPERTIES/CLUSTER BY/semicolon.
                if re.search(r'(?is)\bTBLPROPERTIES\b', target_ddl):
                    target_ddl = re.sub(r'(?is)\bTBLPROPERTIES\b', 'USING DELTA TBLPROPERTIES', target_ddl, count=1)
                elif re.search(r'(?is)\bCLUSTER\s+BY\b', target_ddl):
                    target_ddl = re.sub(r'(?is)\bCLUSTER\s+BY\b', 'USING DELTA CLUSTER BY', target_ddl, count=1)
                else:
                    target_ddl = target_ddl.strip().rstrip(';') + ' USING DELTA;'

            # Clean up trailing semicolons.
            target_ddl = target_ddl.strip().rstrip(';') + ';'

            # CONSTRAINT HANDLING - Match prompt behavior exactly
            # Extract CHECK constraints and move to ALTER TABLE statements
            # Remove UNIQUE constraints entirely (don't create ALTER TABLE for them)
            import re
            
            # Extract table name
            table_match = re.search(r'CREATE\s+TABLE\s+[`"]?([^`"\s(]+)[`"]?', target_ddl, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)
                
                # Extract CHECK constraints
                check_pattern = r'CHECK\s*\([^)]+\)'
                check_matches = re.findall(check_pattern, target_ddl, re.IGNORECASE)
                
                # Extract UNIQUE constraints
                unique_pattern = r'UNIQUE\s*\([^)]+\)'
                unique_matches = re.findall(unique_pattern, target_ddl, re.IGNORECASE)
                
                # Remove all constraints from CREATE TABLE
                target_ddl = re.sub(r',?\s*(CHECK|UNIQUE)\s*\([^)]+\)', '', target_ddl, flags=re.IGNORECASE)
                target_ddl = re.sub(r'\s+', ' ', target_ddl)  # Clean up extra spaces
                target_ddl = re.sub(r'\s*,\s*\)', ')', target_ddl)  # Clean up trailing commas
                target_ddl = re.sub(r'\(\s*,', '(', target_ddl)  # Clean up leading commas
                
                # Add ALTER TABLE statements for CHECK constraints only
                alter_statements = []
                for i, check_constraint in enumerate(check_matches, 1):
                    constraint_name = f"chk_{table_name}_check_{i}".lower()
                    alter_statements.append(f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{constraint_name}` {check_constraint};")
                
                # Combine CREATE TABLE with ALTER TABLE statements
                if alter_statements:
                    target_ddl = target_ddl + "\n\n" + "\n".join(alter_statements)
            
            notes = [
                f"Fallback translation from {source_dialect} to {target_dialect}",
                "Aligned NUMBER mapping to INT/DECIMAL per enterprise rules",
                "Preserved VARCHAR/CHAR lengths",
                "Added USING DELTA to CREATE TABLE",
                "Moved CHECK constraints to ALTER TABLE statements per prompt requirements",
                "Removed UNIQUE constraints entirely as Databricks does not enforce them"
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
