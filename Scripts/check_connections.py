import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
import asyncio

async def check_connections():
    # Get all connections
    connections = await ConnectionModel.get_all()
    print("All connections:")
    for conn in connections:
        print(f"  ID: {conn['id']}, Name: {conn['name']}, Type: {conn['db_type']}")
    
    # Get specific connection details
    target = await ConnectionModel.get_by_id(6)  # PostgreSQL connection
    if target:
        print(f"\nTarget connection details:")
        print(f"  ID: {target['id']}")
        print(f"  Name: {target['name']}")
        print(f"  Type: {target['db_type']}")
        
        # Decrypt credentials
        creds = decrypt_credentials(target["enc_credentials"])
        print(f"  Credentials: {creds}")
    else:
        print("\nTarget connection not found")

if __name__ == "__main__":
    asyncio.run(check_connections())