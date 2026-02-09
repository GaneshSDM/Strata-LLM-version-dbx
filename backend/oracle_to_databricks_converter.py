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
        system_prompt = """You are a database migration expert specialized in converting Oracle DDL to Databricks SQL.

CRITICAL INSTRUCTION FOR TABLE CREATION:
- [MANDATORY] Keep PRIMARY KEY and FOREIGN KEY constraints inside the CREATE TABLE definition
- [MANDATORY] Do NOT move PRIMARY KEY or FOREIGN KEY to ALTER TABLE statements
- The execution layer will handle FK constraints properly (two-phase creation for self-referencing FKs)

Core Conversion Rules:
- Convert schema-qualified names to simple backtick names (e.g., "schema"."table" → `table`)
- NEVER use IDENTIFIER() function in DDL output
- Every CREATE TABLE must include USING DELTA
- Add TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ONLY if DEFAULT keyword is used
- Move CHECK and UNIQUE constraints to separate ALTER TABLE statements
- Keep PRIMARY KEY and FOREIGN KEY inside CREATE TABLE block

Data Type Mappings:
- NUMBER (no precision/scale) → INT
- NUMBER(p) → DECIMAL(p)
- NUMBER(p,s) → DECIMAL(p,s)
- VARCHAR2(n)/NVARCHAR2(n) → VARCHAR(n)
- CHAR(n)/NCHAR(n) → CHAR(n)
- TIMESTAMP (all variants) → TIMESTAMP
- CLOB/NCLOB → STRING (no length)
- BLOB/RAW → BINARY (no length)

Output Format:
- Return valid JSON with this structure:
{
  "objects": [
    {
      "name": "TableName",
      "kind": "table",
      "target_sql": "CREATE TABLE IF NOT EXISTS `TableName` (...) USING DELTA;",
      "notes": []
    }
  ],
  "warnings": []
}

IMPORTANT: Preserve all PRIMARY KEY and FOREIGN KEY constraints in the CREATE TABLE statement."""

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