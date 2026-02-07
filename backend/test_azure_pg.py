"""
Test Azure PostgreSQL connection with SSL
"""
import psycopg2

# Your Azure PostgreSQL credentials
conn_params = {
    "host": "mypostgresdummy.postgres.database.azure.com",
    "port": 5432,
    "database": "postgres",  # Default database - REQUIRED!
    "user": "mydbadmin",
    "password": "YOUR_PASSWORD_HERE",  # Replace with your actual password
    "sslmode": "require"
}

print("Testing Azure PostgreSQL connection...")
print(f"Host: {conn_params['host']}")
print(f"Port: {conn_params['port']}")
print(f"Database: {conn_params['database']}")
print(f"User: {conn_params['user']}")
print(f"SSL Mode: {conn_params['sslmode']}")
print()

try:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print("✅ Connection successful!")
    print(f"PostgreSQL Version: {version}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")
    print()
    print("Common issues:")
    print("1. Wrong password")
    print("2. Database name not specified (Azure requires explicit database)")
    print("3. Firewall rules not allowing your IP address")
    print("4. SSL certificate verification issues")
