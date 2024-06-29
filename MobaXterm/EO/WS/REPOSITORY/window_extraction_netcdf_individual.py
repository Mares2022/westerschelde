"""This code explore a folder with multiple L2W files and extracts pixel windows for a set of stations defined in a Stations.xlsx file"""

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
import dask.array as da

# Define a function to evaluate RAM memory usage.
def print_memory():
    # Get memory information
    memory_info = psutil.virtual_memory()
    print(f"Memory Usage: {memory_info.percent:.2f}%")


# Define a function to extract the date from the path. Data about the year.
def extract_date(path):

    match = re.search(r'(\d{4}_\d{2}_\d{2})', path)
    if match:
        return match.group()
    else:
        return "0000_00_00"  # Return a default date if no date is found
    
# Define function to get the coordinates in the netcdfs closer to the stations coordinates.
def get_exact_coordinates_in_netcdf(dataset, x_coord, y_coord):

    time_series   = dataset.sel(x=x_coord, y=y_coord, method='nearest')
    y_coord_exact = time_series['y'].values.tolist()
    x_coord_exact = time_series['x'].values.tolist()

    return y_coord_exact, x_coord_exact 

# Define function to extract pixel values
def get_window(x_coord, y_coord, stationID, dataset):

   # Get exact coordinates found in the netcdf files.
    y_coord_exact, x_coord_exact = get_exact_coordinates_in_netcdf(dataset, x_coord, y_coord)

    # Get index values or given coordinates
    y_index = np.where(dataset['y'] == y_coord_exact)[0].tolist()[0]
    x_index = np.where(dataset['x'] == x_coord_exact)[0].tolist()[0]

    # Obtaining the central pixel's longitud and latitude.
    central_lon  = x_index
    central_lat  = y_index

    # Get pixel coordinates around the central pixel
    lat_start, lat_end = central_lat - 1, central_lat + 2
    lon_start, lon_end = central_lon - 1, central_lon + 2

    # Select only data inside the window of sample. Return a filtered netcdf (dataset)
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

    #TODO: This component should read both a file created with a Sentinel A and Sentinel B sensors to work. 
    #TODO: The files were manually selected and will not work for other cases in in the same sensor utput is selected.

    ds_start = xr.open_dataset(os.path.join(folder_path, sorted_files[0]))
    ds_end   = xr.open_dataset(os.path.join(folder_path, sorted_files[-1]))

    ds_start = ds_start.drop_vars(variables_to_remove)
    ds_end   = ds_end.drop_vars(variables_to_remove)

    variable_names_start = list(ds_start.variables)
    variable_names_end   = list(ds_end.variables)

    print("The variables in SA derived products are:",variable_names_start)
    print("The variables in SB derived products are:",variable_names_end)

    variable_names = list(set(variable_names_start + variable_names_end))
    print("The variables", variable_names)

    return sorted_files, variable_names

def flag_central_pixel(df, specific_lat, specific_lon, stationID):
    
    # Adding a 1 where Latitude and Longitude match the specified values
    df.loc[(df['Latitude'] == specific_lat) & (df['Longitude'] == specific_lon) & (df['Station'] == str(stationID)), 'flag'] = 5
    
    return df

# Define input and outputs
folder_path_l2w_data   = '/eodc/private/deltares/OUTPUT_10m_2015_2024/'
file_path_stations   = '/home/eodc/EO/WS_PRODUCTION/INPUT/Stations.xlsx' 

# Get files and variable names for S2A and S2B sensors 
sorted_files, variable_names = get_files_and_variable_names(folder_path_l2w_data)

# Define subset for testing. Few images and few variables to read
sorted_files = sorted_files[:]
df_stations = pd.read_excel(file_path_stations)

print(variable_names)

for varnetcdf in variable_names:

  ds_list = []
  variable_names_loop = [varnetcdf]

  print('Selected variables')
  print(varnetcdf)
  file_path_output_nc    =  f'/home/eodc/EO/WS_PRODUCTION/RESULTS/window_extraction_output_{varnetcdf}.nc'
  print('Creating', file_path_output_nc)
  
  for idx, variable_name in enumerate(variable_names_loop):
  
      print('\nCalculated variable: '+ variable_name)

      station_windows_list = []
  
      for file in sorted_files:
          file_path = os.path.join(folder_path_l2w_data, file)
          print("Processing variable:",idx)
          print("Processing variable:", variable_name)
          print("Processing file: ", file_path)
          with xr.open_dataset(file_path, chunks={'x': 1000, 'y': 1000}) as dataset:
              time_series = [datetime.fromisoformat(dataset.attrs.get("isodate"))] 
              
              try:
                  dataset = xr.concat([dataset[variable_name]], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))
                  
                  for index, row in df_stations.iterrows():
                      station_window = get_window(row['x_coord'], row['y_coord'], row['stationID'],dataset)
                      station_windows_list.append(station_window)
                      del station_window    
                      
                  print(f"Memory usage of dataset: {sys.getsizeof(dataset)} bytes")
                  print_memory()   
                  del dataset 
  
              except Exception as e:
                  print(f"The file {file}: {e} does not have the current variable. It will be skipped")
                  continue  
              
      print('Concat windows for multiple years')
      # Create a single netcdf with all the data
      merged_stations = xr.concat(station_windows_list, dim='station').groupby('station').max(dim='station')
      del station_windows_list
      ds_list.append(merged_stations)
  
  # Create a single netcdf
  print('Starting writting netcdf for ',varnetcdf)
  merged_ds = xr.merge(ds_list)
  merged_ds['time'] = merged_ds['time'].astype('datetime64[ns]')
  merged_ds.to_netcdf(file_path_output_nc)

  del merged_stations 
  del merged_ds
  del ds_list

  print('The process is finished')

  




