#!/usr/bin/env python3
"""
Test script to test the API endpoint that frontend uses to get database details
"""

import sys
import os
import asyncio
import json
import aiohttp

async def test_api_endpoint():
    base_url = "http://localhost:8000"
    
    # Test data - simulate what the frontend sends
    test_payload = {
        "connectionId": 18  # Using the Databricks connection ID from the connection list
    }
    
    print("Testing /api/database/details endpoint...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/api/database/details",
                json=test_payload,
                timeout=aiohttp.ClientTimeout(total=30)  # 30 second timeout
            ) as response:
                print(f"Status: {response.status}")
                result = await response.text()
                print("Response:")
                try:
                    # Try to parse as JSON
                    json_result = json.loads(result)
                    print(json.dumps(json_result, indent=2))
                    
                    # Check if it has the expected structure
                    if json_result.get("ok"):
                        data = json_result.get("data", {})
                        
                        # Check for required fields
                        required_top_level = ['database_info', 'tables', 'columns', 'views', 'storage_info', 'data_profiles']
                        missing_fields = [field for field in required_top_level if field not in data]
                        
                        if missing_fields:
                            print(f"\nMISSING TOP-LEVEL FIELDS: {missing_fields}")
                        else:
                            print("\n✓ All required top-level fields present!")
                            
                        # Check specific sub-fields
                        db_info = data.get('database_info', {})
                        db_info_required = ['type', 'version', 'schemas', 'encoding', 'collation']
                        missing_db_info = [field for field in db_info_required if field not in db_info]
                        
                        if missing_db_info:
                            print(f"MISSING database_info fields: {missing_db_info}")
                        else:
                            print("✓ All database_info fields present!")
                            
                        storage_info = data.get('storage_info', {})
                        if 'database_size' in storage_info:
                            size_required = ['total_size', 'data_size', 'index_size']
                            missing_size = [field for field in size_required if field not in storage_info['database_size']]
                            
                            if missing_size:
                                print(f"MISSING storage_info.database_size fields: {missing_size}")
                            else:
                                print("✓ All storage_info.database_size fields present!")
                        
                        print(f"\nResponse summary:")
                        print(f"- OK: {json_result.get('ok')}")
                        print(f"- Database type: {db_info.get('type', 'N/A')}")
                        print(f"- Number of tables: {len(data.get('tables', []))}")
                        print(f"- Number of columns: {len(data.get('columns', []))}")
                        print(f"- Number of views: {len(data.get('views', []))}")
                        print(f"- Has storage_info: {'storage_info' in data}")
                        print(f"- Has data_profiles: {'data_profiles' in data}")
                        
                    else:
                        print(f"API returned error: {json_result.get('message', 'Unknown error')}")
                        
                except json.JSONDecodeError:
                    print("Response is not valid JSON:")
                    print(result)
                    
    except asyncio.TimeoutError:
        print("Request timed out after 30 seconds")
    except Exception as e:
        print(f"Error making request: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_api_endpoint())