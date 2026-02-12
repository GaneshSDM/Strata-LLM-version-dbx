"""
Test script to diagnose and verify Databricks connection issues
"""
import asyncio
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from backend.adapters.databricks import DatabricksAdapter

async def test_databricks_connection():
    """Test Databricks connection with credentials from the UI"""

    print("=" * 80)
    print("Testing Databricks Connection")
    print("=" * 80)

    # Test 1: Credentials as sent from frontend (server_hostname, http_path, access_token)
    print("\n[TEST 1] Testing with frontend field names (server_hostname, http_path, access_token)")
    credentials_frontend = {
        "server_hostname": "dbc-3247cc85-ef1e.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
        "access_token": "dapi445bf722cd9e028f0b331a0513b0a193",
        "catalog": "databricks_catalog_new",
        "schema": "education"
    }

    adapter = DatabricksAdapter(credentials_frontend)
    result = await adapter.test_connection()

    print(f"Result: {result}")
    if result.get("ok"):
        print("[SUCCESS] Connection test passed with frontend field names")
    else:
        print(f"[FAILED] {result.get('message', 'Unknown error')}")
        print(f"Full error details: {result}")

    # Test 2: Credentials with backend field names (host instead of server_hostname)
    print("\n[TEST 2] Testing with backend field names (host, http_path, access_token)")
    credentials_backend = {
        "host": "dbc-3247cc85-ef1e.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
        "access_token": "dapi445bf722cd9e028f0b331a0513b0a193",
        "catalog": "databricks_catalog_new",
        "schema": "education"
    }

    adapter2 = DatabricksAdapter(credentials_backend)
    result2 = await adapter2.test_connection()

    print(f"Result: {result2}")
    if result2.get("ok"):
        print("[SUCCESS] Connection test passed with backend field names")
    else:
        print(f"[FAILED] {result2.get('message', 'Unknown error')}")
        print(f"Full error details: {result2}")

    # Test 3: Credentials with missing fields
    print("\n[TEST 3] Testing with missing access_token")
    credentials_missing = {
        "server_hostname": "dbc-3247cc85-ef1e.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
        # Missing access_token
        "catalog": "databricks_catalog_new",
        "schema": "education"
    }

    adapter3 = DatabricksAdapter(credentials_missing)
    result3 = await adapter3.test_connection()

    print(f"Result: {result3}")
    if not result3.get("ok"):
        print("[EXPECTED] Connection should fail with missing access_token")
        print(f"Error message: {result3.get('message', 'Unknown error')}")
    else:
        print("[UNEXPECTED] Connection should have failed")

    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    print("Review the results above to identify the issue.")
    print("If Test 1 fails but Test 2 passes, the issue is field name mismatch.")
    print("If both fail, the issue is with the credentials or network connectivity.")

if __name__ == "__main__":
    asyncio.run(test_databricks_connection())
