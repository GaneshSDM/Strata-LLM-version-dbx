import requests
import time
import json

def test_structure_status():
    """Test the structure migration status endpoint"""
    print("Testing structure migration status endpoint...")
    
    try:
        response = requests.get('http://localhost:8003/api/migrate/structure-status')
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Parsed JSON: {json.dumps(data, indent=2)}")
            
            if 'progress' in data and 'percent' in data['progress']:
                print(f"Progress percent: {data['progress']['percent']}")
                print(f"Progress phase: {data['progress']['phase']}")
            else:
                print("No progress data found in response")
        else:
            print("Request failed")
            
    except Exception as e:
        print(f"Error testing endpoint: {e}")

def test_data_status():
    """Test the data migration status endpoint"""
    print("\nTesting data migration status endpoint...")
    
    try:
        response = requests.get('http://localhost:8003/api/migrate/data-status')
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Parsed JSON: {json.dumps(data, indent=2)}")
            
            if 'progress' in data:
                print(f"Progress data type: {type(data['progress'])}")
                if isinstance(data['progress'], dict):
                    print("Table progress entries:")
                    for table, progress in data['progress'].items():
                        print(f"  {table}: {progress}")
                else:
                    print(f"Progress data: {data['progress']}")
            else:
                print("No progress data found in response")
        else:
            print("Request failed")
            
    except Exception as e:
        print(f"Error testing endpoint: {e}")

if __name__ == "__main__":
    test_structure_status()
    test_data_status()