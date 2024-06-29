#Explore a folder with multiple L2W files and extracts pixel windows for a set of stations defined in a .csv files

import os
import xarray as xr
from datetime import datetime
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import altair as alt
import numpy as np
import re
import psutil
import sys
# import dask.array as da

def print_memory():
    # Get memory information
    memory_info = psutil.virtual_memory()

    # Print memory information
    # print(f"Total Memory: {memory_info.total / (1024 ** 3):.2f} GB")
    # print(f"Used Memory : {memory_info.used / (1024 ** 3):.2f} GB")
    # print(f"Free Memory : {memory_info.available / (1024 ** 3):.2f} GB")
    print(f"Memory Usage: {memory_info.percent:.2f}%")


# Define a function to extract the date from the path. Data abount the year
def extract_date(path):
    match = re.search(r'(\d{4}_\d{2}_\d{2})', path)
    if match:
        return match.group()
    else:
        return "0000_00_00"  # Return a default date if no date is found

# Define function to extract pixel values
def get_window(x_coord, y_coord, stationID, dataset):
    time_series   = dataset.sel(x=x_coord, y=y_coord, method='nearest')
    y_coord_exact = time_series['y'].values.tolist()
    x_coord_exact = time_series['x'].values.tolist()
    y_index = np.where(dataset['y'] == y_coord_exact)[0].tolist()[0]
    x_index = np.where(dataset['x'] == x_coord_exact)[0].tolist()[0]
    central_lon  = x_index
    central_lat  = y_index

    lat_start, lat_end = central_lat - 1, central_lat + 2
    lon_start, lon_end = central_lon - 1, central_lon + 2

    # window_values = da.from_array(dataset.isel(y=slice(lat_start, lat_end), x=slice(lon_start, lon_end)))
    # window_values = window_values.astype('float32')
    # window_values = window_values.compute()

    window_values = dataset.isel(y=slice(lat_start, lat_end), x=slice(lon_start, lon_end))
    window_values = window_values.expand_dims(station=[stationID])
    
    return window_values

def get_files_and_variable_names(folder_path):
    # List all files in the folder
    files_in_folder = os.listdir(folder_path)

    # Filter the list to include only files ending with "ic.nc"
    filtered_files = [file for file in files_in_folder if file.endswith("L2W.nc")]

    # Sort the paths based on the extracted date
    sorted_files = sorted(filtered_files, key=extract_date)

    variables_to_remove = ['transverse_mercator', 'x', 'y', 'lon', 'lat']
    ds = xr.open_dataset(os.path.join(folder_path, sorted_files[0]))
    ds= ds.drop_vars(variables_to_remove)

    variable_names_A = list(ds.variables)
    print(variable_names_A)

    ds = xr.open_dataset(os.path.join(folder_path, sorted_files[-1]))
    ds= ds.drop_vars(variables_to_remove)

    variable_names_B = list(ds.variables)
    variable_names_B
    print(variable_names_B)

    variable_names = list(set(variable_names_A + variable_names_B))
    print(variable_names)

    return sorted_files, variable_names

# Define input and outputs
folder_path = r'P:\11209243-eo\Window_extraction\INPUT'
folder_path_stations = r'P:\11209243-eo\Window_extraction\INPUT\Stations.xlsx' 

output_nc_file_path =  'OUTPUT/full_file_TEST.nc'
output_excel_file_path = 'OUTPUT/full_pixel_windows_value_TEST.xlsx' 

# Get files and variable names for S2A and S2B sensors 
sorted_files, variable_names = get_files_and_variable_names(folder_path)

# Define subset for testing. Few images and few variables to read
sorted_files = sorted_files[:]
variable_names =variable_names[:]

df_list = []
ds_list = []

for variable_name in variable_names:
    print('\n'+ variable_name+'\n')
    df = pd.read_excel(folder_path_stations)
    stations_list = []

    for file in sorted_files:
        path = os.path.join(folder_path, file)
        print(path)

        dataset = xr.open_dataset(path, chunks={'x': 100, 'y': 100})

        time_series = [datetime.fromisoformat(dataset.attrs.get("isodate"))]  # Replace with your time series data
        
        try:
            dataset = xr.concat([dataset[variable_name]], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))
            
            for index, row in df.iterrows():

                station_window = get_window(row['x_coord'], row['y_coord'], row['stationID'],dataset)
                stations_list.append(station_window)
                del station_window    
            
            print(f"Memory usage of dataset: {sys.getsizeof(dataset)} bytes")
            print_memory()   
            del dataset 

        except Exception as e:
            print(f"Error with {file}: {e}")
            continue  

    # Create a single netcdf with all the data

    merged_stations = xr.concat(stations_list, dim='station').groupby('station').max(dim='station')
    del stations_list

    # Get the latitude and longitude coordinates
    latitudes = merged_stations['y'].values
    longitudes = merged_stations['x'].values
    times = merged_stations['time'].values
    stations = merged_stations['station'].values

    # Initialize lists to store data
    pixel_values = []
    coordinates = []

    # Iterate over latitudes and longitudes
    for station_index, station in enumerate(stations):
        print(station)
        for time_index, time in enumerate(times):
            for lat_index, lat in enumerate(latitudes):
                for lon_index, lon in enumerate(longitudes):
                    
                    # Extract pixel value
                    ##pixel_value = merged_stations[variable_name].isel(y=lat_index, x=lon_index, time=time_index, station=station_index).values.item()
                    pixel_value = merged_stations.sel(station=station).isel(y=lat_index, x=lon_index, time=time_index).values

                    time = pd.Timestamp(time).tz_localize(None)

                    # Append data to lists
                    pixel_values.append(pixel_value)
                    coordinates.append((lat, lon, station, time))

    df = pd.DataFrame({
        'Latitude': [coord[0] for coord in coordinates],
        'Longitude': [coord[1] for coord in coordinates],
        'Station': [coord[2] for coord in coordinates],
        'Time': [coord[3] for coord in coordinates],
        variable_name: pixel_values
    })

    df['ID'] =  df['Latitude'].round().astype(int).astype(str) + '_' + df['Longitude'].round().astype(int).astype(str) + '_' + df['Station'] + '_' + df['Time'].dt.strftime('%Y_%m_%d')

    df_list.append(df)
    ds_list.append(merged_stations)

    print(f"Memory usage of df_list: {sys.getsizeof(df_list)} bytes")
    print(f"Memory usage of ds_list: {sys.getsizeof(ds_list)} bytes")

    del df
    del merged_stations

# Create a single table 
merged_df = df_list[0].iloc[:,4:6]  

for df in df_list[1:]:
    #merged_df = pd.merge(merged_df, df.iloc[:,4:5], left_index=True, right_index=True, how='left')
    merged_df = pd.merge(merged_df, df.iloc[:,4:6], on='ID', how='outer')

split_columns = merged_df['ID'].str.split('_', expand=True)
merged_df.insert(0, 'Latitude', split_columns[0].astype(float))
merged_df.insert(1, 'Longitude', split_columns[1].astype(float))
merged_df.insert(2, 'Station', split_columns[2].astype(str))
merged_df.insert(3, 'Year', split_columns[3].astype(str))
merged_df.insert(4, 'Month', split_columns[4].astype(str))
merged_df.insert(5, 'Day', split_columns[5].astype(str))

merged_df.insert(6, 'Date', pd.to_datetime(merged_df[['Year', 'Month', 'Day']], errors='coerce').dt.date)

merged_df = merged_df.replace('nan', np.nan)
merged_df.set_index('ID', inplace=True)
excel_output_path = 'full_pixel_windows_value.xlsx' #This miss to integrate extra columns with metadata (e.g. station ID)
merged_df.to_excel(output_excel_file_path,na_rep='nan')

# Create a single netcdf
merged_ds =xr.merge(ds_list)
merged_ds['time'] = merged_ds['time'].astype('datetime64')
merged_ds.to_netcdf(output_nc_file_path)

