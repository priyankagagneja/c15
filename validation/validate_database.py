#!/usr/bin/env python3
"""
View SQLite database contents
"""

import sqlite3

# Connect to the database
conn = sqlite3.connect('data/weather_data.db')

# Get list of tables
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("Tables in the database:")
for table in tables:
    table_name = table[0]
    print(f"\n=== {table_name} ===")
    
    # Get table schema
    cursor.execute(f"PRAGMA table_info({table_name})")
    schema = cursor.fetchall()
    columns = [col[1] for col in schema]
    print("Schema:", ", ".join(columns))
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Row count: {count}")
    
    # Get sample data (first 5 rows)
    if count > 0:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        rows = cursor.fetchall()
        print("\nSample data (first 5 rows):")
        for row in rows:
            print(row)

# Close connection
conn.close()
