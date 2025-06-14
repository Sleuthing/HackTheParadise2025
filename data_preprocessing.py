import pandas as pd

def load_data(file_full_path: str, file_name: str = "selected_data.csv"):
    """
    Load data from a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.
    """

    try:
        data = pd.read_csv(file_full_path)
        # select only few columns for simplicity
        data = data[['timestamp', 'sensor', 'value']]
    
        # str convert timestamp class 'str' to datetime
        if data['timestamp'].dtype == 'object':
            data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')
        # convert timestamp to datetime and format it
        data['timestamp'] = pd.to_datetime(data['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        data.to_csv(f"{file_name}.csv", index=False)

        
    except Exception as e:
        print(f"Error loading data: {e}")
        # return pd.DataFrame() 


# create a function to process the data and create a new file with max, min, mean values
# new file to create max, min, mean values based on timestamp minute
def process_data(file_full_path: str, file_name: str = "processed_data.csv"):
    """
    Process data from a CSV file to calculate max, min, and mean values based on timestamp minute.

    Args:
        file_full_path (str): The path to the CSV file.
        file_name (str): The name of the output CSV file.

    Returns:
        pd.DataFrame: The processed data as a DataFrame.
    """
    
    try:
        data = pd.read_csv(file_full_path)
        
        # Convert timestamp to datetime
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        sensor_name = data['sensor'].unique()[0].replace(" ", "_")  
        print(sensor_name)

        # Group by timestamp minute and sensor, then calculate sensorname_max, sensorname_min, sensorname_mean
        processed_data = data.groupby([data['timestamp'].dt.floor('min'), 'sensor']).agg(
            max_value=('value', 'max'),
            min_value=('value', 'min'),
            mean_value=('value', 'mean')
        ).reset_index()

        # Rename columns with sensor name as prefix
        processed_data.rename(columns={
            'max_value': f'{sensor_name}_max',
            'min_value': f'{sensor_name}_min',
            'mean_value': f'{sensor_name}_mean'
        }, inplace=True)
        
        # Delete the 'sensor' column as it is no longer needed
        processed_data.drop(columns=['sensor'], inplace=True)
        # Save processed data to a new CSV file
        processed_data.to_csv(f"D:/StadtKlima/src/data_preprocessing/feature_engineering/{file_name}.csv", index=False)
        
    except Exception as e:
        print(f"Error processing data: {e}")


def create_master_file(file_paths: list, master_file_name: str = "master_data.csv"):
    """
    Create a master CSV file from individual CSV files.

    Args:
        file_paths (list): List of paths to the individual CSV files.
        master_file_name (str): The name of the output master CSV file.

    Returns:
        pd.DataFrame: The combined data as a DataFrame.
    """

    master_data = pd.DataFrame()

    for file_path in file_paths:
        # data = pd.read_csv(file_path)
        print(file_path)

        data = pd.read_csv(file_path)
        master_data = pd.concat([master_data, data], axis=1)
    
    # remove duplicate columns
    master_data = master_data.loc[:, ~master_data.columns.duplicated()]
    master_data.rename(columns={
            'value': 'Ambient_Light_Sensor_max'
        }, inplace=True)
    # drop the 'sensor' column if it exists
    if 'sensor' in master_data.columns:
        master_data.drop(columns=['sensor'], inplace=True)

    # make timestamp as index
    master_data['timestamp'] = pd.to_datetime(master_data['timestamp'])
    master_data.set_index('timestamp', inplace=True)
    print(type(master_data.index))

    # filter the data based on the timestamp column
    start_date = pd.to_datetime("2025-04-01 12:54:00")
    end_date = pd.to_datetime("2025-05-30 21:30:00")
    master_data = master_data.loc[start_date:end_date]

    # Save the master data to a CSV file
    master_data.to_csv(master_file_name, index=True)


    
# create full file path
file_dir = "D:/StadtKlima/Datasource/E40B8669B602_sensors_data/E40B8669B602_sensors_data/"
sensor_file_path = "Temp_E40B8669B602_data_20250401-20250531.csv" # provide file name one by one

full_full_path = file_dir + sensor_file_path
# print(f"Loading data from: {full_full_path}")
load_data(full_full_path, "Temp_E40B8669B602") # provide file name one by one


# create full file path
file_dir = "D:/StadtKlima/src/data_preprocessing/"
sensor_file_path = "Temp_E40B8669B602.csv" # provide file name one by one

full_full_path = file_dir + sensor_file_path
# print(f"Loading data from: {full_full_path}")
process_data(full_full_path, "Temp") # provide file name one by one


file_dir = "D:/StadtKlima/src/data_preprocessing/feature_engineering/"
file_paths = [
    "Barometer.csv",
    "BVOC.csv",
    "CO2.csv",
    "relHum.csv",
    "Temp.csv",
    "Light_E40B8669B602.csv"
    # Add more file names as needed
]
file_paths = [file_dir + file for file in file_paths]
create_master_file(file_paths)
