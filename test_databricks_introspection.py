#!/usr/bin/env python3
"""
Test script to specifically test Databricks introspection method
"""

import sys
import os
import asyncio
import json

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from adapters.databricks import DatabricksAdapter

async def test_databricks_introspection():
    # Use the same credentials as the user's Databricks connection
    credentials = {
        "server_hostname": "adb-5553828747870112.10.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/b58cbf87092b7321",
        "access_token": "dapi2b6b04c72616d4206753047498927189",  # This should be the real token from your connection
        "catalog": "hive_metastore",
        "schema": "default"
    }

    adapter = DatabricksAdapter(credentials)
    
    print("Testing Databricks introspection...")
    try:
        result = await adapter.introspect_analysis()
        print("Introspection result:")
        print(json.dumps(result, indent=2, default=str))
        
        # Check for required fields that the frontend expects
        required_fields = ['database_info', 'tables', 'columns', 'views', 'storage_info', 'data_profiles']
        missing_fields = []
        for field in required_fields:
            if field not in result:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"\nMISSING FIELDS: {missing_fields}")
        else:
            print("\nAll required fields present!")
            
        # Check specific sub-fields that frontend expects
        if 'database_info' in result:
            db_info_required = ['type', 'version', 'schemas', 'encoding', 'collation']
            missing_db_info = []
            for field in db_info_required:
                if field not in result['database_info']:
                    missing_db_info.append(field)
            if missing_db_info:
                print(f"Missing database_info fields: {missing_db_info}")
                
        if 'storage_info' in result and 'database_size' in result['storage_info']:
            size_required = ['total_size', 'data_size', 'index_size']
            missing_size = []
            for field in size_required:
                if field not in result['storage_info']['database_size']:
                    missing_size.append(field)
            if missing_size:
                print(f"Missing storage_info.database_size fields: {missing_size}")
                
        print(f"\nResult summary:")
        print(f"- Database type: {result.get('database_info', {}).get('type', 'N/A')}")
        print(f"- Number of tables: {len(result.get('tables', []))}")
        print(f"- Number of columns: {len(result.get('columns', []))}")
        print(f"- Number of views: {len(result.get('views', []))}")
        print(f"- Has storage_info: {'storage_info' in result}")
        print(f"- Has data_profiles: {'data_profiles' in result}")
        
    except Exception as e:
        print(f"Error during introspection: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_databricks_introspection())