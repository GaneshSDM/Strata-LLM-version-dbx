import asyncio
import os
import sys

# Ensure backend modules can be imported when running from repo root
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from backend.adapters.oracle import OracleAdapter

BASE_CREDENTIALS = {
    "port": 1521,
    "service_name": "xepdb1",
    "username": "gova",
    "password": "Gova@12345",
}

HOSTS_TO_TEST = [
    "DESKTOP-GMLKEL3",
    "180.151.248.10",
]


async def test_host(hostname: str):
    credentials = dict(BASE_CREDENTIALS)
    credentials["host"] = hostname
    adapter = OracleAdapter(credentials)
    return hostname, await adapter.test_connection()


async def main():
    for host in HOSTS_TO_TEST:
        hostname, result = await test_host(host)
        print(f"{hostname}: {result}")


if __name__ == "__main__":
    asyncio.run(main())
