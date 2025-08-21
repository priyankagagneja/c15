#!/usr/bin/env python3
"""
View geocoded station data from the SQLite database
"""

import sqlite3
import pandas as pd
from tabulate import tabulate

# Connect to the database
conn = sqlite3.connect('data/weather_data.db')
cursor = conn.cursor()

# Query states with geocoded stations and count
query = """
SELECT s.name as state_name, COUNT(st.id) as geocoded_count
FROM states s
JOIN stations st ON s.id = st.state_id
WHERE st.latitude IS NOT NULL AND st.longitude IS NOT NULL
GROUP BY s.name
ORDER BY geocoded_count DESC
"""

print("=== States with Geocoded Stations ===")
cursor.execute(query)
states_data = cursor.fetchall()
print(tabulate(states_data, headers=["State", "Geocoded Stations"], tablefmt="pretty"))

# Query geocoded stations details
query = """
SELECT 
    s.name as state,
    st.city,
    st.code,
    st.location,
    st.latitude,
    st.longitude
FROM 
    stations st
JOIN 
    states s ON st.state_id = s.id
WHERE 
    st.latitude IS NOT NULL AND st.longitude IS NOT NULL
ORDER BY
    s.name, st.city
"""

print("\n=== Geocoded Station Details ===")
cursor.execute(query)
stations_data = cursor.fetchall()

try:
    # Try to use tabulate for prettier output
    print(tabulate(stations_data, 
                  headers=["State", "City", "Code", "Location", "Latitude", "Longitude"], 
                  tablefmt="pretty"))
except:
    # Fallback to simple printing if tabulate isn't available
    print("State, City, Code, Location, Latitude, Longitude")
    for row in stations_data:
        print(", ".join(map(str, row)))

# Get total geocoding statistics
cursor.execute("SELECT COUNT(*) FROM stations")
total_stations = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM stations WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
geocoded_stations = cursor.fetchone()[0]

print(f"\nGeocoding Statistics:")
print(f"Total stations: {total_stations}")
print(f"Geocoded stations: {geocoded_stations}")
print(f"Geocoding completion: {geocoded_stations/total_stations*100:.2f}%")

# Close connection
conn.close()
