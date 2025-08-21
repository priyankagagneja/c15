import pandas as pd
import sqlite3
import plotly.express as px
import sys

DB_PATH = 'data/weather_data.db'
TABLE = 'weather_records'

def get_station_data(station_code, y_col):
    # Connect to the SQLite database
    conn = sqlite3.connect(DB_PATH)
    
    # Query to get wind speed data for the given station_id
    query = f"""
    SELECT date, {y_col}
    FROM weather_records
    WHERE station_code = '{station_code}'
    ORDER BY date;
    """
    
    # Read the data into a pandas DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Close the database connection
    conn.close()
    
    return df

def plot_ts(station_code, y_col):

    df = get_station_data(station_code, y_col)

    col_nm = y_col.replace('_', ' ').title()

    if df.empty:
        print(f"No data found for station: {station_code}")
    else:
        fig = px.line(
            df,
            x='date',
            y=y_col,
            title=f'{col_nm} over time for {station_code} Station',
            labels={'date': 'Date', y_col: y_col.replace('_', ' ').title()}
        )
        fig.update_traces(mode='lines+markers')
        fig.update_layout(xaxis_title='Date', yaxis_title=y_col.replace('_', ' ').title())
        fig.show()

# Method 1
# User selects station and column to plot
# station_code = 'HSV' # Huntsville, AL ; MOB = Mobile, AL ; BHM = Birmingham, AL ; MGM = Montgomery, AL
# y_col = 'avg_temp'   # precipitation, avg_temp, max_temp, min_temp, wind_direction, wind_speed

# df = get_station_data(station_code, y_col)
# print(df.head())
# plot_ts(df, station_code, y_col)

# Method 2
# plot_ts(station_code = 'MOB', y_col = 'precipitation')  # Example usage
# plot_ts(station_code = 'BHM', y_col = 'min_temp')  # Example usage

# Method 3
station_code = sys.argv[1]
y_col = sys.argv[2]
plot_ts(station_code, y_col)
