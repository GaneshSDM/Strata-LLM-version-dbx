#!/usr/bin/env python3
"""
Test script to debug Databricks database details API endpoint
"""
import asyncio
import aiohttp
import json

async def test_databricks_details():
    # First get all connections to find a Databricks one
    connections_url = "http://127.0.0.1:8000/api/connections"
    
    print("Getting list of connections...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(connections_url) as response:
                connections_data = await response.json()
                if not connections_data.get("ok"):
                    print("Failed to get connections")
                    return
                
                connections = connections_data.get("data", [])
                print(f"Found {len(connections)} connections:")
                databricks_connections = []
                
                for conn in connections:
                    print(f"  ID: {conn['id']}, Name: {conn['name']}, Type: {conn['db_type']}")
                    if conn['db_type'] == 'Databricks':
                        databricks_connections.append(conn)
                
                if not databricks_connections:
                    print("\n❌ No Databricks connections found!")
                    return
                
                # Use the first Databricks connection
                databricks_conn = databricks_connections[0]
                connection_id = databricks_conn['id']
                print(f"\nUsing Databricks connection: {databricks_conn['name']} (ID: {connection_id})")
    
    except Exception as e:
        print(f"Error getting connections: {e}")
        return
    
    # Now test the database details API
    url = "http://127.0.0.1:8000/api/database/details"
    
    payload = {
        "connectionId": connection_id
    }
    
    print(f"\nTesting Databricks database details API...")
    print(f"URL: {url}")
    print(f"Payload: {payload}")
    print("-" * 50)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                print(f"Status Code: {response.status}")
                print(f"Headers: {dict(response.headers)}")
                
                # Get response content
                content = await response.text()
                print(f"Raw Response: {content}")
                
                try:
                    data = json.loads(content)
                    print(f"Parsed JSON Response:")
                    print(json.dumps(data, indent=2))
                    
                    if data.get("ok"):
                        print("\n✅ SUCCESS: API returned ok=true")
                        database_info = data.get("data", {}).get("database_info", {})
                        print(f"Database Type: {database_info.get('type')}")
                        print(f"Database Version: {database_info.get('version')}")
                        print(f"Schemas Found: {len(database_info.get('schemas', []))}")
                        print(f"Encoding: {database_info.get('encoding')}")
                        print(f"Collation: {database_info.get('collation')}")
                                            
                        tables = data.get("data", {}).get("tables", [])
                        print(f"Tables Found: {len(tables)}")
                                            
                        columns = data.get("data", {}).get("columns", [])
                        print(f"Columns Found: {len(columns)}")
                                            
                        views = data.get("data", {}).get("views", [])
                        print(f"Views Found: {len(views)}")
                                            
                        constraints = data.get("data", {}).get("constraints", [])
                        print(f"Constraints Found: {len(constraints)}")
                                            
                        procedures = data.get("data", {}).get("procedures", [])
                        print(f"Procedures Found: {len(procedures)}")
                                            
                        indexes = data.get("data", {}).get("indexes", [])
                        print(f"Indexes Found: {len(indexes)}")
                                            
                        data_profiles = data.get("data", {}).get("data_profiles", [])
                        print(f"Data Profiles Found: {len(data_profiles)}")
                                            
                        storage_info = data.get("data", {}).get("storage_info", {})
                        print(f"Storage Info Available: {bool(storage_info)}")
                        if storage_info:
                            db_size = storage_info.get("database_size", {})
                            print(f"Database Size Info: {bool(db_size)}")
                            print(f"Total Database Size: {db_size.get('total_size', 0)}")
                            print(f"Data Size: {db_size.get('data_size', 0)}")
                            print(f"Index Size: {db_size.get('index_size', 0)}")
                                                
                            table_storage = storage_info.get("tables", [])
                            print(f"Table Storage Info: {len(table_storage)}")
                    else:
                        print(f"\n❌ ERROR: API returned ok=false")
                        print(f"Error Message: {data.get('message', 'No message')}")
                        print(f"Error Details: {data.get('error', 'No details')}")
                        
                except json.JSONDecodeError as e:
                    print(f"\n❌ JSON DECODE ERROR: {e}")
                    print(f"Response content is not valid JSON")
                    
    except Exception as e:
        print(f"\n❌ REQUEST ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(test_databricks_details())