import asyncio
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import init_db, ConnectionModel

async def test_connections():
    # Initialize the database
    await init_db()
    print("Database initialized")
    
    # Test getting connections
    connections = await ConnectionModel.get_all()
    print("Connections:", connections)
    
    if connections:
        print("✅ SUCCESS: Connections retrieved correctly")
        for conn in connections:
            print(f"  - {conn['name']} ({conn['db_type']})")
    else:
        print("ℹ️  INFO: No connections found (this might be expected)")

if __name__ == "__main__":
    asyncio.run(test_connections())