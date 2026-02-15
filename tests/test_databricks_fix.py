#!/usr/bin/env python3
"""
Test script to check if the Databricks adapter now returns the same structure as other adapters
"""
import sys
import os
import asyncio
import json
import aiohttp

async def test_databricks_api():
    base_url = "http://localhost:8000"
    
    # Test the database details endpoint with a Databricks connection ID
    # Since we don't know the exact ID, let's first test the connections endpoint
    print("Testing /api/connections endpoint...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base_url}/api/connections") as response:
                print(f"Connections Status: {response.status}")
                connections_data = await response.json()
                print("Connections Response:")
                print(json.dumps(connections_data, indent=2))
                
                if connections_data.get("ok") and connections_data.get("data"):
                    connections_list = connections_data["data"]
                    # Look for Databricks connections
                    databricks_connections = [conn for conn in connections_list 
                                              if conn.get("db_type") == "Databricks"]
                    print(f"\nFound {len(databricks_connections)} Databricks connections")
                    
                    if databricks_connections:
                        # Test the first Databricks connection
                        databricks_conn = databricks_connections[0]
                        connection_id = databricks_conn['id']
                        print(f"Testing Databricks connection: {databricks_conn['name']} (ID: {connection_id})")
                        
                        # Test the database details API
                        details_payload = {
                            "connectionId": connection_id
                        }
                        
                        print(f"\nTesting database details API for connection ID: {connection_id}")
                        async with session.post(
                            f"{base_url}/api/database/details",
                            json=details_payload
                        ) as details_response:
                            print(f"Details Status: {details_response.status}")
                            
                            if details_response.status == 200:
                                details_data = await details_response.json()
                                print("Details Response:")
                                print(json.dumps(details_data, indent=2))
                                
                                if details_data.get("ok"):
                                    print("\n✅ SUCCESS: Databricks API is working!")
                                    data = details_data.get("data", {})
                                    
                                    # Check for required fields that frontend expects
                                    required_fields = [
                                        "database_info", "tables", "columns", "constraints", 
                                        "views", "procedures", "indexes", "data_profiles", 
                                        "storage_info"
                                    ]
                                    
                                    missing_fields = []
                                    for field in required_fields:
                                        if field not in data:
                                            missing_fields.append(field)
                                        elif data[field] is None:
                                            missing_fields.append(field)
                                    
                                    if not missing_fields:
                                        print("✅ All required fields are present!")
                                        print(f"   - Database type: {data['database_info'].get('type')}")
                                        print(f"   - Schemas: {data['database_info'].get('schemas', [])}")
                                        print(f"   - Tables: {len(data.get('tables', []))}")
                                        print(f"   - Columns: {len(data.get('columns', []))}")
                                        print(f"   - Views: {len(data.get('views', []))}")
                                        print(f"   - Data Profiles: {len(data.get('data_profiles', []))}")
                                        print(f"   - Constraints: {len(data.get('constraints', []))}")
                                        print(f"   - Procedures: {len(data.get('procedures', []))}")
                                        print(f"   - Indexes: {len(data.get('indexes', []))}")
                                    else:
                                        print(f"❌ Missing fields: {missing_fields}")
                                        
                                        # Let's check what's actually in the data
                                        print(f"   Actual fields in response: {list(data.keys())}")
                                        for field in required_fields:
                                            if field in data:
                                                field_val = data[field]
                                                if isinstance(field_val, (list, dict)):
                                                    count = len(field_val) if isinstance(field_val, list) else len(field_val.keys()) if isinstance(field_val, dict) else 'N/A'
                                                    print(f"   ✓ {field}: {type(field_val).__name__}, count: {count}")
                                                else:
                                                    print(f"   ✓ {field}: {type(field_val).__name__}, value: {field_val}")
                                            else:
                                                print(f"   ❌ {field}: Missing")
                                        
                                    # Check if it has the same structure as MySQL/PostgreSQL
                                    print("\n🔍 Comparing structure with other databases...")
                                    db_info = data.get("database_info", {})
                                    print(f"   Database info keys: {list(db_info.keys())}")
                                    print(f"   Storage info keys: {list(data.get('storage_info', {}).keys())}")
                                    
                                    # Check if tables have expected fields
                                    if data.get('tables'):
                                        first_table = data['tables'][0] if data['tables'] else {}
                                        print(f"   Sample table keys: {list(first_table.keys())}")
                                    
                                    # Check if columns have expected fields
                                    if data.get('columns'):
                                        first_col = data['columns'][0] if data['columns'] else {}
                                        print(f"   Sample column keys: {list(first_col.keys())}")
                                else:
                                    print(f"❌ Details API returned error: {details_data.get('message', 'Unknown error')}")
                            else:
                                print(f"❌ Details API returned status {details_response.status}")
                                error_text = await details_response.text()
                                print(f"Error response: {error_text}")
                    else:
                        print("No Databricks connections found")
                else:
                    print("No connections found or error in connections API")
                    
    except Exception as e:
        print(f"Error testing API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_databricks_api())