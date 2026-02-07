import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models import ConnectionModel
import asyncio

async def get_connections():
    connections = await ConnectionModel.get_all()
    print('Available connections:')
    for conn in connections:
        print(f'ID: {conn["id"]}, Name: {conn["name"]}, Type: {conn["db_type"]}')
    return connections

if __name__ == "__main__":
    connections = asyncio.run(get_connections())
    print(f"\nTotal connections found: {len(connections)}")