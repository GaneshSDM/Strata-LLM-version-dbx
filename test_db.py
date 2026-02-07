import asyncio
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import init_db, SessionModel

async def test_db_operations():
    # Initialize the database
    await init_db()
    print("Database initialized")
    
    # Test setting selected tables
    await SessionModel.set_selected_tables(['users', 'orders'])
    print("Set selected tables")
    
    # Test getting session
    session = await SessionModel.get_session()
    print("Retrieved session:", session)
    
    if session and session.get('selected_tables'):
        print("✅ SUCCESS: Selected tables retrieved correctly")
        print("Selected tables:", session['selected_tables'])
    else:
        print("❌ FAILURE: Could not retrieve selected tables")

if __name__ == "__main__":
    asyncio.run(test_db_operations())