import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

import mysql.connector
import psycopg2
from backend.models import ConnectionModel
from backend.encryption import decrypt_credentials
import asyncio

async def check_employee_data():
    # Get source connection (MySQL)
    source = await ConnectionModel.get_by_id(5)  # MySQL connection
    source_creds = decrypt_credentials(source["enc_credentials"])
    
    # Get target connection (PostgreSQL)
    target = await ConnectionModel.get_by_id(6)  # PostgreSQL connection
    target_creds = decrypt_credentials(target["enc_credentials"])
    
    print("Checking employee data in source (MySQL) and target (PostgreSQL)...")
    
    try:
        # Check source data
        mysql_conn = mysql.connector.connect(
            host=source_creds.get("host"),
            port=source_creds.get("port", 3306),
            database=source_creds.get("database"),
            user=source_creds.get("username"),
            password=source_creds.get("password"),
        )
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        mysql_cursor.execute("SELECT emp_id, emp_name, dept_id FROM `newdb`.`employees` ORDER BY emp_id")
        mysql_rows = mysql_cursor.fetchall()
        
        print(f"\nSource (MySQL) - employees table:")
        print(f"  Row count: {len(mysql_rows)}")
        print(f"  Data:")
        for row in mysql_rows:
            print(f"    emp_id: {row['emp_id']} ({type(row['emp_id'])}), emp_name: {row['emp_name']}, dept_id: {row['dept_id']}")
        
        mysql_cursor.close()
        mysql_conn.close()
        
        # Check target data
        pg_conn = psycopg2.connect(
            host=target_creds.get("host"),
            port=target_creds.get("port", 5432),
            database=target_creds.get("database"),
            user=target_creds.get("username"),
            password=target_creds.get("password"),
            sslmode=target_creds.get("sslmode", "disable")
        )
        pg_cursor = pg_conn.cursor()
        
        pg_cursor.execute("SELECT emp_id, emp_name, dept_id FROM employees ORDER BY emp_id")
        pg_rows = pg_cursor.fetchall()
        
        print(f"\nTarget (PostgreSQL) - employees table:")
        print(f"  Row count: {len(pg_rows)}")
        print(f"  Data:")
        for row in pg_rows:
            print(f"    emp_id: {row[0]} ({type(row[0])}), emp_name: {row[1]}, dept_id: {row[2]}")
        
        pg_cursor.close()
        pg_conn.close()
        
        # Compare data
        print(f"\nData Comparison:")
        if len(mysql_rows) == len(pg_rows):
            print(f"  ✅ Row counts match: {len(mysql_rows)}")
        else:
            print(f"  ❌ Row count mismatch: MySQL={len(mysql_rows)}, PostgreSQL={len(pg_rows)}")
            
        # Check if data values match
        data_matches = True
        for i, (mysql_row, pg_row) in enumerate(zip(mysql_rows, pg_rows)):
            if (mysql_row['emp_id'] != pg_row[0] or 
                mysql_row['emp_name'] != pg_row[1] or 
                mysql_row['dept_id'] != pg_row[2]):
                print(f"  ❌ Data mismatch at row {i+1}")
                data_matches = False
        
        if data_matches and len(mysql_rows) == len(pg_rows):
            print(f"  ✅ All data values match perfectly")
            
    except Exception as e:
        print(f"Error checking data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_employee_data())