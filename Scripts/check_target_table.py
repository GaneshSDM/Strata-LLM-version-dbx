import psycopg2

# PostgreSQL connection details
pg_config = {
    'host': 'mypostgresdummy.postgres.database.azure.com',
    'port': 5432,
    'database': 'newdb',
    'user': 'mydbadmin',
    'password': 'decisionminds@123',
    'sslmode': 'require'
}

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_config)
    cursor = conn.cursor()
    
    # Check if departments table exists and get column info from all schemas
    cursor.execute("""
        SELECT table_schema, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'departments'
        ORDER BY table_schema, ordinal_position
    """)
    
    columns = cursor.fetchall()
    
    print(f"Target table 'departments' columns:")
    print(f"  Column count: {len(columns)}")
    for col in columns:
        print(f"  - {col[0]}.{col[1]} ({col[2]})")
    
    if len(columns) == 0:
        print("  Table 'departments' does not exist or has no columns")
    
    # Check if table exists in all schemas
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name = 'departments'
    """)
    
    tables = cursor.fetchall()
    print(f"\nTable locations:")
    for table in tables:
        print(f"  - {table[0]}.{table[1]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")