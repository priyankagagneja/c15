import pandas as pd

CSV_FILE = "data/weather_parsed.csv"

# Load the CSV data as a DataFrame
print(f"Loading {CSV_FILE}...")
df = pd.read_csv(CSV_FILE)

##### Simple profiling with pandas #####
def profile_csv(df):
    
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print("\nColumn Types:")
    print(df.dtypes)
    print("\nMissing Values:")
    print(df.isnull().sum())
    print("\nBasic Statistics (numeric columns):")
    print(df.describe())
    print("\nUnique values in categorical columns:")
    for col in df.select_dtypes(include=['object']):
        print(f"{col}: {df[col].nunique()} unique values")
    print("\nSample rows:")
    print(df.head())

if __name__ == "__main__":
    profile_csv(df)




##### Detailed profiling with YDataProfiling #####
from ydata_profiling import ProfileReport

# Drop columns with all NaN values (prevents profiling errors)
df = df.dropna(axis=1, how='all')
df = df.convert_dtypes()

# create and save the profile report
print("**Generating detailed YDataProfiling profile report**")
report = ProfileReport(df)
report.to_file("weather_profile_report.html")

