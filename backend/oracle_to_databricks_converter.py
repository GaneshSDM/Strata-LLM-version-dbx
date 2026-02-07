import os
import json
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

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

# Use the model from environment variable, defaulting to gpt-4o-mini if not set
model = AI_INTEGRATIONS_OPENAI_MODEL or "gpt-4o-mini"
print(f"[AI MODULE] Using model: {model}")

async def translate_schema(source_dialect: str, target_dialect: str, input_ddl_json: dict) -> dict:
    if not client:
        print("[AI] Client not available for translation")
        return {
            "objects": [],
            "warnings": ["OpenAI client not initialized. Using fallback translation."],
            "error": "OpenAI client not available"
        }
    
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
        - Try to not put special characters in variable names. If you have to include special characters in key, or include semicolon in value, please Use backquotes, e.g., SET `key`=`value`"""

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
        print(f"[AI] Translation error: {str(e)}")
        print(f"[AI] Error type: {type(e).__name__}")
        import traceback
        print(f"[AI] Full traceback: {traceback.format_exc()}")
        return {
            "objects": [],
            "warnings": [f"AI translation error: {str(e)}. Using fallback translation."],
            "error": str(e)
        }