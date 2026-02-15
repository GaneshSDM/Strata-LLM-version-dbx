import mysql.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters (these should match your test database)
config = {
    'host': 'localhost',
    'port': 3306,
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'password'),
    'database': os.getenv('MYSQL_DATABASE', 'newdb')
}

try:
    # Connect to database
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)
    
    db_name = config['database']
    
    print(f"Connected to database: {db_name}")
    
    # Query tables using the old method (TABLE_ROWS estimate)
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TABLE_ROWS
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
    """, (db_name,))
    
    print("\n=== ESTIMATED ROW COUNTS (TABLE_ROWS) ===")
    for row in cursor.fetchall():
        if row['TABLE_TYPE'] == 'BASE TABLE':
            print(f"Table: {row['TABLE_SCHEMA']}.{row['TABLE_NAME']} - Estimated rows: {row['TABLE_ROWS'] or 0}")
    
    # Query tables using the new method (actual COUNT(*))
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
    """, (db_name,))
    
    tables = cursor.fetchall()
    
    print("\n=== ACTUAL ROW COUNTS (COUNT(*)) ===")
    for table in tables:
        if table['TABLE_TYPE'] == 'BASE TABLE':
            try:
                count_cursor = conn.cursor()
                count_cursor.execute(f"SELECT COUNT(*) FROM `{table['TABLE_SCHEMA']}`.`{table['TABLE_NAME']}`")
                actual_count = count_cursor.fetchone()[0]
                count_cursor.close()
                print(f"Table: {table['TABLE_SCHEMA']}.{table['TABLE_NAME']} - Actual rows: {actual_count}")
            except Exception as e:
                print(f"Error getting count for {table['TABLE_SCHEMA']}.{table['TABLE_NAME']}: {e}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")