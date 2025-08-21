# Cadence Weather Data Analysis 

### Features
- Reading a pickle format binary file
- CSV data loading into normalized SQLite database
- Geocoding of weather stations by state using Nominatim
- Efficient data processing using SQLAlchemy ORM
- Progress tracking with tqdm

### Project Structure

- `get_data.py`: script - reads binary data and converts to csv
- `profile_data.py`: script - profiles to better understand the data
- `create_db_pipeline.py`: script - implements data pipeline and geocodes for _Alabama_
- `plot_time_series.py`: script - plots individual variable for a station, taking user inputs
- `weather_parsed.csv`: data - Input weather data
- `weather_data.db`: data - Output SQLite database (created when running the pipeline)

### Setup
1. Install uv package manager:
```bash
pip install uv
```

2. Create a virtual environment and install dependencies:
```bash
uv venv
```

## 1. Get data    

Data available [here](https://github.com/reubenfirmin/interview_data/blob/master/weather.data) is downloaded and parsed as:

```bash
uv run src/get_data.py 
```

## 2. Understand Data (Profiling)

I have used both pandas and ydata-profiling packages to understand the nuances of the data.

```bash
uv run src/profile_data.py
```

Output contains 
- Data types and statistics for each column

Additionaly, this will create a `.html` file conaining:
- Null value analysis
- Unique value counts
- Sample values
- Data quality alerts

This report helps better understand the data pretty quickly and also identify potential issues before further processing.


## 3. Weather Data Pipeline

Implementation of data pipeline that 
- loads the data from CSV into a normalized SQLite database and 
- provides geocoding functionality for weather stations.


### Database Schema

The normalized database schema consists of:

1. **States**: Contains state information
   - code (PK)
   - name

2. **Stations**: Contains weather station information
   - code (PK)
   - state_code (FK to States)
   - city
   - location
   - latitude (added by geocoding)
   - longitude (added by geocoding)

3. **WeatherRecords**: Contains weather measurements
   - id (PK)
   - precipitation
   - avg_temp, max_temp, min_temp
   - wind_direction, wind_speed
   - date, year, month, week_of
   - station_code (FK to Stations)
   - state_code


**Run the pipeline**

```bash
uv run src/create_db_pipeline.py
```

### Customization

- By default, the pipeline geocodes stations in Alabama.
- To geocode stations in a different state, modify the `main()` function in `pipeline.py`.



## 4. Visualising time series patterns 

   Potential values for 
   - station_code = HSV, MOB, BHM, MGM # Huntsville, Mobile, Birmingham,Montgomery stations in AL
   - y_col = avg_temp, precipitation, avg_temp, max_temp, min_temp, wind_direction, wind_speed

```bash
uv run src/plot_time_series.py MOB precipitation
```

## Validation scripts 

**To view the database**

```bash
uv run validation/validate_database.py
```
**To check geocoding**
```bash
uv run validation/validate_geocoded_data.py
```

## Next Steps

If I had more time at hand I would also attempt at doing modeling but skipping it for lack of time at hand.