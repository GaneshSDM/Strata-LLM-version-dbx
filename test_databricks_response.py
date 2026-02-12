# Test script to diagnose Databricks LLM response format
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from ai import _call_databricks_translation, _extract_llm_content
import json

# Simple test DDL
test_input = {
    "objects": [{
        "name": "TEST_TABLE",
        "kind": "table",
        "schema": "PUBLIC",
        "source_ddl": "CREATE TABLE TEST (ID NUMBER(10));"
    }]
}

system_prompt = "Convert Oracle DDL to Databricks SQL."

print("Testing Databricks LLM response extraction...")
print("=" * 50)

# First test _extract_llm_content with a mock response
mock_openai_response = {
    "choices": [{
        "message": {
            "content": "CREATE TABLE `TEST` (`ID` DECIMAL(10,0)) USING DELTA;"
        }
    }]
}

print(f"Mock OpenAI response: {json.dumps(mock_openai_response, indent=2)}")
content = _extract_llm_content(mock_openai_response)
print(f"Extracted: '{content}'")
print()

# Test with Databricks-style response
mock_databricks_response = {
    "predictions": [{
        "content": "CREATE TABLE `TEST` (`ID` DECIMAL(10,0)) USING DELTA;"
    }]
}

print(f"Mock Databricks response: {json.dumps(mock_databricks_response, indent=2)}")
content = _extract_llm_content(mock_databricks_response)
print(f"Extracted: '{content}'")
print()

# Now try actual API call
print("=" * 50)
print("Testing actual Databricks API call...")
try:
    result = _call_databricks_translation(system_prompt, test_input)
    print(f"Success! Objects returned: {len(result.get('objects', []))}")
    if result.get('objects'):
        print(f"First object target_sql length: {len(result['objects'][0].get('target_sql', ''))}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
