import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
from backend.adapters import get_adapter
import asyncio

async def test_direct_connection():
    # Get the connection details
    source = await ConnectionModel.get_by_id(5)  # MySQL connection
    source_creds = decrypt_credentials(source["enc_credentials"])
    
    print(f"Connecting to MySQL database:")
    print(f"Host: {source_creds.get('host')}")
    print(f"Port: {source_creds.get('port')}")
    print(f"Database: {source_creds.get('database')}")
    
    # Create adapter
    source_adapter = get_adapter(source["db_type"], source_creds)
    
    # Test direct row count for t_99
    try:
        row_count = await source_adapter.get_table_row_count("t_99")
        print(f"\nDirect row count for t_99: {row_count}")
    except Exception as e:
        print(f"\nError getting row count: {e}")
    
    # Test introspect analysis
    try:
        analysis_result = await source_adapter.introspect_analysis()
        print(f"\nAnalysis tables:")
        for table in analysis_result.get("tables", []):
            print(f"  {table['schema']}.{table['name']}: {table['row_count']} rows")
    except Exception as e:
        print(f"\nError in introspect_analysis: {e}")

if __name__ == "__main__":
    asyncio.run(test_direct_connection())