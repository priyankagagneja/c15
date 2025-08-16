# Cadence Weather Data Analysis 

### Features
- Reading a pickle format binary file
- CSV data loading into normalized SQLite database
- Geocoding of weather stations by state using Nominatim
- Efficient data processing using SQLAlchemy ORM
- Progress tracking with tqdm

### Project Structure

- `01_get_data.py`: script - reads binary data and converts to csv
- `02_profile_data.py`: script - profiles to better understand the data
- `03_create_db_pipeline.py`: script - implements data pipeline and geocodes for _Alabama_
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

Data available [here](https://github.com/reubenfirmin/interview
data/blob/master/weather.data) is downloaded and parsed as:

```bash
uv run 01_get_data.py 
```

## 2. Understand Data (Profiling)

I have used both pandas and ydata-profiling packages to understand the nuances of the data.

```bash
uv run 02_profile_data.py
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

A Python data pipeline that loads weather data from CSV into a normalized SQLite database and provides geocoding functionality for weather stations.


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
python pipeline.py
```

### Customization

- By default, the pipeline geocodes stations in Alabama.
- To geocode stations in a different state, modify the `main()` function in `pipeline.py`.


## Next Steps

Next, I am looking to add:
- modeling script,
- visualization scripts,

and potentially move the main scripts into and `src` folder and add `validation` folder for validation scripts (which have been written but not been added to github yet)