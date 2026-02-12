"""
Comprehensive test for Databricks connection flow
Tests the entire flow: Frontend -> Sanitize -> Adapter -> Connection
"""
import asyncio
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from backend.adapters.databricks import DatabricksAdapter
from backend.main import _sanitize_credentials

async def test_comprehensive_flow():
    """Test the complete flow from frontend to backend"""

    print("=" * 80)
    print("Comprehensive Databricks Connection Test")
    print("=" * 80)

    # Test 1: Simulate frontend sending credentials (as they appear in ConnectionModal)
    print("\n[TEST 1] Frontend credentials -> Sanitize -> Adapter")
    print("-" * 80)

    frontend_credentials = {
        "server_hostname": "dbc-3247cc85-ef1e.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
        "access_token": "dapi445bf722cd9e028f0b331a0513b0a193",
        "catalog": "databricks_catalog_new",
        "schema": "education"
    }

    print("Step 1: Frontend sends credentials")
    print(f"  server_hostname: {frontend_credentials['server_hostname']}")
    print(f"  http_path: {frontend_credentials['http_path']}")
    print(f"  access_token: ***")
    print(f"  catalog: {frontend_credentials['catalog']}")
    print(f"  schema: {frontend_credentials['schema']}")

    # Step 2: Sanitize credentials (happens in main.py)
    sanitized = _sanitize_credentials("Databricks", frontend_credentials)
    print("\nStep 2: Sanitize credentials")
    print(f"  Sanitized: {list(sanitized.keys())}")
    print(f"  server_hostname present: {bool(sanitized.get('server_hostname'))}")
    print(f"  http_path present: {bool(sanitized.get('http_path'))}")
    print(f"  access_token present: {bool(sanitized.get('access_token'))}")

    # Step 3: Create adapter and test connection
    print("\nStep 3: Create adapter and test connection")
    adapter = DatabricksAdapter(sanitized)
    result = await adapter.test_connection()

    print(f"  Result: {result.get('ok')}")
    print(f"  Message: {result.get('message')}")
    if result.get("ok"):
        print("  [SUCCESS] Complete flow works!")
    else:
        print(f"  [FAILED] {result.get('message')}")

    # Test 2: Test with whitespace in credentials (should be stripped)
    print("\n[TEST 2] Credentials with whitespace")
    print("-" * 80)

    whitespace_credentials = {
        "server_hostname": "  dbc-3247cc85-ef1e.cloud.databricks.com  ",
        "http_path": "  /sql/1.0/warehouses/ea7ff8660b900b78  ",
        "access_token": "  dapi445bf722cd9e028f0b331a0513b0a193  ",
        "catalog": "  databricks_catalog_new  ",
        "schema": "  education  "
    }

    sanitized2 = _sanitize_credentials("Databricks", whitespace_credentials)
    print(f"  Whitespace stripped: {len(sanitized2['server_hostname']) < len(whitespace_credentials['server_hostname'])}")

    adapter2 = DatabricksAdapter(sanitized2)
    result2 = await adapter2.test_connection()

    if result2.get("ok"):
        print("  [SUCCESS] Whitespace handling works!")
    else:
        print(f"  [FAILED] {result2.get('message')}")

    # Test 3: Test with empty strings (should fail with clear message)
    print("\n[TEST 3] Empty server_hostname (should fail)")
    print("-" * 80)

    empty_credentials = {
        "server_hostname": "   ",  # Empty/whitespace only
        "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
        "access_token": "dapi445bf722cd9e028f0b331a0513b0a193",
        "catalog": "databricks_catalog_new",
        "schema": "education"
    }

    sanitized3 = _sanitize_credentials("Databricks", empty_credentials)
    adapter3 = DatabricksAdapter(sanitized3)
    result3 = await adapter3.test_connection()

    if not result3.get("ok") and "hostname" in result3.get("message", "").lower():
        print(f"  [EXPECTED] Clear error message: {result3.get('message')}")
    else:
        print(f"  [UNEXPECTED] Result: {result3}")

    # Test 4: Test with alternative field names (backward compatibility)
    print("\n[TEST 4] Alternative field names (host instead of server_hostname)")
    print("-" * 80)

    alt_credentials = {
        "host": "dbc-3247cc85-ef1e.cloud.databricks.com",  # Using 'host' instead
        "httpPath": "/sql/1.0/warehouses/ea7ff8660b900b78",  # Using camelCase
        "accessToken": "dapi445bf722cd9e028f0b331a0513b0a193",  # Using camelCase
        "catalogName": "databricks_catalog_new",  # Using alternative name
        "schemaName": "education"  # Using alternative name
    }

    sanitized4 = _sanitize_credentials("Databricks", alt_credentials)
    print(f"  Normalized to server_hostname: {bool(sanitized4.get('server_hostname'))}")
    print(f"  Normalized to http_path: {bool(sanitized4.get('http_path'))}")
    print(f"  Normalized to access_token: {bool(sanitized4.get('access_token'))}")

    adapter4 = DatabricksAdapter(sanitized4)
    result4 = await adapter4.test_connection()

    if result4.get("ok"):
        print("  [SUCCESS] Backward compatibility works!")
    else:
        print(f"  [FAILED] {result4.get('message')}")

    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    all_passed = all([
        result.get("ok"),
        result2.get("ok"),
        not result3.get("ok"),  # This should fail
        result4.get("ok")
    ])

    if all_passed:
        print("[SUCCESS] All tests passed! The Databricks connection flow is working correctly.")
    else:
        print("[WARNING] Some tests failed. Review the results above.")

if __name__ == "__main__":
    asyncio.run(test_comprehensive_flow())
