"""
Test the actual API endpoint to simulate frontend behavior
"""
import requests
import json

def test_databricks_api_endpoint():
    """Test the /api/connections/test endpoint with Databricks credentials"""

    print("=" * 80)
    print("Testing Databricks API Endpoint")
    print("=" * 80)

    # Simulate the exact payload the frontend sends
    payload = {
        "dbType": "Databricks",
        "name": "data",
        "credentials": {
            "server_hostname": "dbc-3247cc85-ef1e.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/ea7ff8660b900b78",
            "access_token": "dapi445bf722cd9e028f0b331a0513b0a193",
            "catalog": "databricks_catalog_new",
            "schema": "education"
        }
    }

    print("\n[TEST] Sending POST request to http://localhost:8000/api/connections/test")
    print(f"Payload: {json.dumps({**payload, 'credentials': {**payload['credentials'], 'access_token': '***'}}, indent=2)}")

    try:
        response = requests.post(
            "http://localhost:8000/api/connections/test",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        print(f"\nHTTP Status Code: {response.status_code}")
        print(f"Response: {response.json()}")

        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                print("\n[SUCCESS] API test connection passed!")
                print(f"Message: {data.get('message')}")
                print(f"Version: {data.get('vendorVersion')}")
            else:
                print("\n[FAILED] API test connection failed!")
                print(f"Error: {data.get('message')}")
        else:
            print(f"\n[ERROR] HTTP error: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.ConnectionError as e:
        print(f"\n[ERROR] Cannot connect to backend server at http://localhost:8000")
        print("Is the backend server running?")
        print("Run: python backend/main.py")
        print(f"Details: {e}")
    except requests.exceptions.Timeout:
        print(f"\n[ERROR] Request timed out after 30 seconds")
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_databricks_api_endpoint()
