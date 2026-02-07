import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
import mysql.connector
import asyncio

async def check_table_contents():
    # Get the connection details
    source = await ConnectionModel.get_by_id(5)  # MySQL connection
    source_creds = decrypt_credentials(source["enc_credentials"])
    
    print(f"Connecting to MySQL database:")
    print(f"Host: {source_creds.get('host')}")
    print(f"Port: {source_creds.get('port')}")
    print(f"Database: {source_creds.get('database')}")
    
    # Connect directly to database
    try:
        conn = mysql.connector.connect(
            host=source_creds.get("host"),
            port=source_creds.get("port", 3306),
            database=source_creds.get("database"),
            user=source_creds.get("username"),
            password=source_creds.get("password"),
        )
        cursor = conn.cursor(dictionary=True)
        
        # Check t_67 table
        cursor.execute("SELECT COUNT(*) as count FROM `newdb`.`t_67`")
        count_result = cursor.fetchone()
        print(f"\nActual row count for t_67: {count_result['count']}")
        
        cursor.execute("SELECT * FROM `newdb`.`t_67`")
        rows = cursor.fetchall()
        print(f"Actual rows in t_67:")
        for i, row in enumerate(rows, 1):
            print(f"  Row {i}: {row}")
            
        # Check t_99 table
        cursor.execute("SELECT COUNT(*) as count FROM `newdb`.`t_99`")
        count_result = cursor.fetchone()
        print(f"\nActual row count for t_99: {count_result['count']}")
        
        cursor.execute("SELECT * FROM `newdb`.`t_99`")
        rows = cursor.fetchall()
        print(f"Actual rows in t_99:")
        for i, row in enumerate(rows, 1):
            print(f"  Row {i}: {row}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nError checking table contents: {e}")

if __name__ == "__main__":
    asyncio.run(check_table_contents())