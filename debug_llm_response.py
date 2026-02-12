# Standalone test for LLM response extraction
import json
import requests
import os
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Updated extraction function
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

# Test with mock responses
print("Testing _extract_llm_content with different formats:")
print("=" * 60)

# OpenAI format
test1 = {"choices": [{"message": {"content": "SELECT 1"}}]}
print(f"OpenAI format: {_extract_llm_content(test1)!r}")

# Databricks predictions format
test2 = {"predictions": [{"content": "SELECT 1"}]}
print(f"Databricks predictions: {_extract_llm_content(test2)!r}")

# Databricks with generated_text
test3 = {"predictions": [{"generated_text": "SELECT 1"}]}
print(f"Databricks generated_text: {_extract_llm_content(test3)!r}")

# Raw text
test4 = {"text": "SELECT 1"}
print(f"Raw text: {_extract_llm_content(test4)!r}")

print("\n" + "=" * 60)
print("Now testing actual Databricks API call...")

url = os.environ.get("DATABRICKS_LLM_INVOCATIONS_URL")
token = os.environ.get("DATABRICKS_LLM_TOKEN")

if not url or not token:
    print("ERROR: DATABRICKS_LLM_INVOCATIONS_URL or DATABRICKS_LLM_TOKEN not set")
    exit(1)

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

payload = {
    "messages": [
        {"role": "system", "content": "Convert Oracle DDL to Databricks SQL."},
        {"role": "user", "content": "Convert this Oracle DDL to Databricks SQL:\nCREATE TABLE TEST (ID NUMBER(10));"}
    ],
    "temperature": 0.1,
    "max_tokens": 8192
}

try:
    response = requests.post(url, headers=headers, json=payload, timeout=45)
    print(f"Status code: {response.status_code}")
    body = response.json()
    
    print(f"\nRaw response keys: {list(body.keys())}")
    print(f"Full response: {json.dumps(body, indent=2)[:2000]}")
    
    content = _extract_llm_content(body)
    print(f"\nExtracted content: {content!r}")
    
    if not content:
        print("\nERROR: Empty content extracted!")
        print("The Databricks endpoint returned data but in an unrecognized format.")
    else:
        print(f"\nSuccess! Extracted {len(content)} chars")
except Exception as e:
    print(f"API Error: {e}")
    import traceback
    traceback.print_exc()
