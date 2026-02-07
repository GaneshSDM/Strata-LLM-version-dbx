import requests
import json

# Test the selective table filtering
try:
    # First, set selected tables
    selected_tables_response = requests.post('http://localhost:8002/api/session/set-selected-tables', 
                                           json={'selectedTables': ['users', 'orders']})
    print("Set selected tables response:", selected_tables_response.status_code, selected_tables_response.json())
    
    # Get session after setting selected tables only
    session_response_before = requests.get('http://localhost:8002/api/session')
    print("Get session (before setting source/target) response:", session_response_before.status_code, session_response_before.json())
    
    # Create a session first (needed for get_session to work)
    session_setup_response = requests.post('http://localhost:8002/api/session/set-source-target',
                                          json={'sourceId': 1, 'targetId': 2})
    print("Set session response:", session_setup_response.status_code, session_setup_response.json())
    
    # Then get session to verify selected tables are stored
    session_response = requests.get('http://localhost:8002/api/session')
    print("Get session response:", session_response.status_code, session_response.json())
    
    # Check if selected tables are in the session
    session_data = session_response.json()
    if session_data.get('ok') and session_data.get('data'):
        selected_tables_in_session = session_data['data'].get('selected_tables', [])
        print("Selected tables in session:", selected_tables_in_session)
        
        if selected_tables_in_session == ['users', 'orders']:
            print("✅ SUCCESS: Selected tables are correctly stored in session")
        else:
            print("❌ FAILURE: Selected tables not correctly stored")
    else:
        print("❌ FAILURE: Could not retrieve session data")
        
except Exception as e:
    print("Error testing selective table filtering:", str(e))
    import traceback
    traceback.print_exc()