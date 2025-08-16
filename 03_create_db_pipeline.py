#!/usr/bin/env python3
"""
Weather Data Pipeline

This script loads weather data from a CSV file into an SQLite database
and provides geocoding functionality for weather stations by state.
"""

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, Date, ForeignKey, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from geopy.geocoders import Nominatim
from tqdm import tqdm

# Constants
DATABASE_PATH = 'data/weather_data.db'
CSV_FILE = 'data/weather_parsed.csv'

# Create SQLAlchemy base
Base = declarative_base()

# Define database models for normalized schema
class State(Base):
    __tablename__ = 'states'
    
    code = Column(String, primary_key=True)  # State code (e.g., 'AL' for Alabama)
    name = Column(String, unique=True, nullable=False)
    
    # Relationships
    stations = relationship("Station", back_populates="state")

class Station(Base):
    __tablename__ = 'stations'
    
    code = Column(String, primary_key=True)  # Station code (e.g., 'BHM' for Birmingham)
    state_code = Column(String, ForeignKey('states.code'), primary_key=True)  # Part of composite primary key
    city = Column(String, nullable=False)
    location = Column(String, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    # Relationships
    state = relationship("State", back_populates="stations")
    weather_records = relationship("WeatherRecord", back_populates="station")
    
    # Define a unique constraint on the composite primary key
    __table_args__ = (
        UniqueConstraint('code', 'state_code', name='uq_station_code_state'),
    )

class WeatherRecord(Base):
    __tablename__ = 'weather_records'
    
    id = Column(Integer, primary_key=True)  # Keep ID for uniqueness of individual records
    precipitation = Column(Float)
    avg_temp = Column(Integer)
    max_temp = Column(Integer)
    min_temp = Column(Integer)
    wind_direction = Column(Integer)
    wind_speed = Column(Float)
    date = Column(Date, nullable=False)
    year = Column(Integer)
    month = Column(Integer)
    week_of = Column(Integer)
    station_code = Column(String, nullable=False)  # Station code
    state_code = Column(String, nullable=False)    # State code
    
    # Foreign key constraint to the composite key in stations table
    __table_args__ = (
        ForeignKeyConstraint(
            ['station_code', 'state_code'], 
            ['stations.code', 'stations.state_code']
        ),
    )
    
    # Relationships
    station = relationship("Station", back_populates="weather_records",
                         foreign_keys=[station_code, state_code])


def get_state_codes_from_csv(csv_file=CSV_FILE):
    """
    Extract state codes from the CSV file and return a dictionary mapping state names to codes.
    
    Args:
        csv_file (str): Path to the CSV file containing weather data.
        
    Returns:
        dict: A dictionary mapping state names to their corresponding codes.
    """
    # Read CSV file
    print(f"Reading {csv_file} to extract state codes...")
    df = pd.read_csv(csv_file)
    
    # Create a mapping dictionary
    state_codes = {}
    state_code_to_name = {}  # Track which full name goes with each code
    
    # First pass: Get all states and their codes from the location column
    for state_name, group in df.groupby('state'):
        # Skip if this state name is actually a code (like 'VA' instead of 'Virginia')
        if len(state_name) == 2 and state_name.isupper():
            continue
            
        # Find the state code from any location entry
        for location in group['location'].unique():
            if ', ' in location:
                state_code = location.split(', ')[1]
                state_codes[state_name] = state_code
                state_code_to_name[state_code] = state_name
                break
    
    # Clean up any states that appear as both code and full name
    # (e.g., both "VA" and "Virginia" in the data)
    for state_name in list(state_codes.keys()):
        state_code = state_codes[state_name]
        
        # If this state's code appears as a state name itself, remove it
        if state_code in state_codes and state_code != state_name:
            print(f"Note: Found '{state_code}' as both a state code and a state name. Using full name '{state_name}'.")
            del state_codes[state_code]
    
    print(f"Found codes for {len(state_codes)} states.")
    return state_codes


class WeatherPipeline:
    def __init__(self):
        """Initialize the pipeline with database connection."""
        self.engine = create_engine(f'sqlite:///{DATABASE_PATH}')
        self.Session = sessionmaker(bind=self.engine)
        self.geolocator = Nominatim(user_agent="weather-data-pipeline")
    
    def create_schema(self):
        """Create the database schema if it doesn't exist."""
        Base.metadata.create_all(self.engine)
        print("Database schema created.")
    
    def load_data(self):
        """Load data from CSV into the SQLite database."""
        # Read CSV file
        print(f"Loading data from {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        
        # Create session
        session = self.Session()
        
        # Get state codes mapping using the dedicated function
        state_mapping = get_state_codes_from_csv(CSV_FILE)
        
        # Process and insert data
        print("Processing states")
        inserted_codes = set()  # Track codes we've already inserted
            
        for state_name in tqdm(df['state'].unique()):
            # Skip state names that are actually codes (like 'DE' rather than 'Delaware')
            if len(state_name) == 2 and state_name.isupper():
                print(f"Skipping abbreviated state name: {state_name}")
                continue
                
            # Check if this state already exists in the database
            state = session.query(State).filter_by(name=state_name).first()
            if not state:
                # Get state code from mapping or use first two letters as fallback
                state_code = state_mapping.get(state_name)
                
                if not state_code:
                    # If no code found in mapping, use first two letters as fallback
                    state_code = state_name[:2].upper()
                    print(f"Warning: Using fallback code {state_code} for {state_name}")
                
                # Check if this code has already been used
                if state_code in inserted_codes:
                    print(f"Skipping duplicate state code: {state_code} for {state_name}")
                    continue
                
                # Check if this code already exists in the database
                existing_state = session.query(State).filter_by(code=state_code).first()
                if existing_state:
                    print(f"Skipping duplicate state code: {state_code} already used by {existing_state.name}")
                    continue
                    
                # Insert the new state
                state = State(code=state_code, name=state_name)
                session.add(state)
                inserted_codes.add(state_code)
            
        # Commit states
        session.commit()
        
        print("Processing stations")
        # Track stations by their code and state_code for later use with weather records
        stations_map = {}
            
        for _, row in tqdm(df[['city', 'code', 'location', 'state']].drop_duplicates().iterrows()):
            # Get state code for the state name
            state_code = state_mapping.get(row['state'], row['state'][:2].upper())
            
            # Check if this station already exists in the database
            station = session.query(Station).filter_by(code=row['code'], state_code=state_code).first()
            
            if not station:
                station = Station(
                    city=row['city'],
                    code=row['code'],
                    location=row['location'],
                    state_code=state_code
                )
                session.add(station)
            
            # Store both station code and state code for weather records
            stations_map[row['code']] = {
                'station_code': row['code'],
                'state_code': state_code
            }
            
        # Commit stations
        session.commit()
        
        print("Processing weather records")
        # Convert date_full to datetime objects
        df['date'] = pd.to_datetime(df['date_full'])
        
        # Process in chunks to avoid memory issues
        chunk_size = 1000
        for i in tqdm(range(0, len(df), chunk_size)):
            chunk = df.iloc[i:i+chunk_size]
            
            weather_records = []
            for _, row in chunk.iterrows():
                station_info = stations_map[row['code']]
                weather_record = WeatherRecord(
                    precipitation=row['precipitation'],
                    avg_temp=row['avg_temp'],
                    max_temp=row['max_temp'],
                    min_temp=row['min_temp'],
                    wind_direction=row['wind_direction'],
                    wind_speed=row['wind_speed'],
                    date=row['date'],
                    year=row['year'],
                    month=row['month'],
                    week_of=row['week_of'],
                    station_code=station_info['station_code'],
                    state_code=station_info['state_code']
                )
                weather_records.append(weather_record)
            
            session.bulk_save_objects(weather_records)
            session.commit()
            
        print("Data loaded successfully!")
            
        # Close the session when done
        session.close()
    
    def geocode_state(self, state_name):
        """
        Geocode all stations within a specified state.
        
        Args:
            state_name (str): Name of the state to geocode stations for.
        """
        print(f"Geocoding stations in {state_name}")
        session = self.Session()
        
        try:
            # Get state and related stations
            state = session.query(State).filter_by(name=state_name).first()
            if not state:
                print(f"State '{state_name}' not found in database.")
                return
            
            stations = session.query(Station).filter_by(state_code=state.code).all()
            print(f"Found {len(stations)} stations to geocode.")
            
            # Geocode each station
            for station in tqdm(stations):
                if station.latitude is not None and station.longitude is not None:
                    print(f"Station {station.code} already has coordinates. Skipping.")
                    continue
                
                try:
                    # Use location for geocoding (e.g., "Birmingham, AL")
                    location = station.location
                    geocode_result = self.geolocator.geocode(location, timeout=10)
                    
                    if geocode_result:
                        station.latitude = geocode_result.latitude
                        station.longitude = geocode_result.longitude
                        print(f"Geocoded {station.location}: ({station.latitude}, {station.longitude})")
                    else:
                        print(f"Could not geocode {station.location}")
                    
                except Exception as e:
                    print(f"Error geocoding {station.location}: {str(e)}")
            
            # Commit changes
            session.commit()
            print(f"Geocoding for {state_name} complete.")
            
        except Exception as e:
            session.rollback()
            print(f"Error during geocoding: {str(e)}")
        finally:
            session.close()


def main():
    """Main function to run the pipeline."""
    pipeline = WeatherPipeline()
    
    # Create schema
    pipeline.create_schema()
    
    # Load data
    pipeline.load_data()
    
    # Example: Geocode a single state (Alabama)
    # Note: We're still using state name for the geocode_state function
    # as the function internally looks up the state code
    pipeline.geocode_state("Alabama")


if __name__ == "__main__":
    main()
