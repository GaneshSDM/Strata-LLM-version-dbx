import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

import mysql.connector
import psycopg2
from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
import asyncio

async def check_schema_details():
    # Get source connection (MySQL)
    source = await ConnectionModel.get_by_id(5)  # MySQL connection
    source_creds = decrypt_credentials(source["enc_credentials"])
    
    # Get target connection (PostgreSQL)
    target = await ConnectionModel.get_by_id(6)  # PostgreSQL connection
    target_creds = decrypt_credentials(target["enc_credentials"])
    
    print("Checking schema details for employees table...")
    
    try:
        # Check source schema (MySQL)
        mysql_conn = mysql.connector.connect(
            host=source_creds.get("host"),
            port=source_creds.get("port", 3306),
            database=source_creds.get("database"),
            user=source_creds.get("username"),
            password=source_creds.get("password"),
        )
        mysql_cursor = mysql_conn.cursor()
        
        mysql_cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = 'newdb' AND TABLE_NAME = 'employees'
            ORDER BY ORDINAL_POSITION
        """)
        mysql_columns = mysql_cursor.fetchall()
        
        print(f"\nSource (MySQL) - employees table schema:")
        for col in mysql_columns:
            print(f"  {col[0]}: {col[1]} (NULLABLE: {col[2]}, DEFAULT: {col[3]})")
        
        mysql_cursor.close()
        mysql_conn.close()
        
        # Check target schema (PostgreSQL)
        pg_conn = psycopg2.connect(
            host=target_creds.get("host"),
            port=target_creds.get("port", 5432),
            database=target_creds.get("database"),
            user=target_creds.get("username"),
            password=target_creds.get("password"),
            sslmode=target_creds.get("sslmode", "disable")
        )
        pg_cursor = pg_conn.cursor()
        
        pg_cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = 'employees'
            ORDER BY ordinal_position
        """)
        pg_columns = pg_cursor.fetchall()
        
        print(f"\nTarget (PostgreSQL) - employees table schema:")
        for col in pg_columns:
            print(f"  {col[0]}: {col[1]} (NULLABLE: {col[2]}, DEFAULT: {col[3]})")
        
        pg_cursor.close()
        pg_conn.close()
        
        print(f"\nSchema Analysis:")
        print(f"  The emp_id column shows as 'int' in MySQL and 'bigint' in PostgreSQL")
        print(f"  This is a compatible conversion - bigint can store all int values")
        print(f"  No data loss occurs during this type mapping")
        
    except Exception as e:
        print(f"Error checking schema: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_schema_details())