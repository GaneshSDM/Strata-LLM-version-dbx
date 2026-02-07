import requests, json
payload = { columnRenames: {public.table: {old: new}}}
r = requests.post('http://localhost:8000/api/session/set-column-renames', json=payload)
print(r.status_code)
print(r.text)
