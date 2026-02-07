import requests
import json

# Test the API directly
response = requests.post("http://localhost:8000/api/database/details", 
                         json={"connectionId": 18})

print("Status:", response.status_code)
data = response.json()
print("Keys in response:", list(data.keys()))

if data.get("ok") and "data" in data:
    inner_data = data["data"]
    print("Keys in inner data:", list(inner_data.keys()))
    
    # Check for specific fields
    fields_to_check = ["constraints", "procedures", "indexes", "data_profiles"]
    for field in fields_to_check:
        if field in inner_data:
            print(f"✓ {field}: Present (type: {type(inner_data[field])}, length: {len(inner_data[field]) if isinstance(inner_data[field], (list, dict)) else 'N/A'})")
        else:
            print(f"✗ {field}: Missing")
else:
    print("Error in response:", data)