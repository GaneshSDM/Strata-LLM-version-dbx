import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
from backend.adapters import get_adapter
import asyncio

async def test_schema_structure():
    # Get the target connection details
    target = await ConnectionModel.get_by_id(6)  # PostgreSQL connection
    target_creds = decrypt_credentials(target["enc_credentials"])
    
    print(f"Target connection details:")
    print(f"  Type: {target['db_type']}")
    print(f"  Credentials: {target_creds}")
    
    # Create adapter
    target_adapter = get_adapter(target["db_type"], target_creds)
    
    # Test the get_schema_structure method with multiple tables
    # This simulates what's being passed in the validation
    tables_ddl = [
        {
            "name": "departments",
            "schema": "newdb"  # Schema mismatch between MySQL and PostgreSQL
        },
        {
            "name": "t_96",
            "schema": "newdb"  # Schema mismatch between MySQL and PostgreSQL
        }
    ]
    
    print(f"\nTesting get_schema_structure with:")
    print(f"  tables_ddl: {tables_ddl}")
    
    try:
        schema_result = await target_adapter.get_schema_structure(tables_ddl)
        print(f"\nSchema structure result:")
        print(f"  {schema_result}")
        
        # Check what columns were found for each table
        for table_dict in tables_ddl:
            table_name = table_dict["name"]
            table_cols = schema_result.get(table_name, [])
            print(f"\n{table_name} columns found: {len(table_cols)}")
            for col in table_cols:
                print(f"  - {col['name']} ({col['type']})")
            
    except Exception as e:
        print(f"\nError in get_schema_structure: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_schema_structure())