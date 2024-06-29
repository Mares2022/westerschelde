#Explore a folder with multiple L2W files and extracts pixel windows for a set of stations defined in a .csv files

import os
import xarray as xr
from datetime import datetime
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import altair as alt
import numpy as np

folder_path = '/eodc/private/deltares/OUTOUT_10m_2015_2024/'
folder_path_stations = '/eodc/private/deltares/Stations.xlsx'   

# List all files in the folder
files_in_folder = os.listdir(folder_path)

# Filter the list to include only files ending with "ic.nc"
filtered_files = [file for file in files_in_folder if file.endswith("L2W.nc")]

filtered_files.sort()

datasets_list = []
# Print the list of matching files
for file in filtered_files:
    path = os.path.join(folder_path , file)
    print(path)
    dataset = xr.open_dataset(path)
    time_series = [datetime.fromisoformat(dataset.attrs.get("isodate"))]  # Replace with your time series data
    ##new_dataset = xr.concat([dataset["chl_re_gons"]], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))
    new_dataset = xr.concat([dataset], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))
    datasets_list.append(new_dataset)

merged_data = xr.concat(datasets_list, dim='time')

def get_window(x_coord, y_coord, stationID):
    time_series = merged_data.sel(x=x_coord, y=y_coord, method='nearest')
    y_coord_exact = time_series['y'].values.tolist()
    x_coord_exact = time_series['x'].values.tolist()
    y_index = np.where(merged_data['y'] == y_coord_exact)[0].tolist()[0]
    x_index = np.where(merged_data['x'] == x_coord_exact)[0].tolist()[0]
    central_lon  = x_index
    central_lat  = y_index

    lat_start, lat_end = central_lat - 1, central_lat + 2
    lon_start, lon_end = central_lon - 1, central_lon + 2

    window_values = merged_data.isel(y=slice(lat_start, lat_end), x=slice(lon_start, lon_end))
    window_values = window_values.expand_dims(station=[stationID])

    return window_values

# Read list of stations
df = pd.read_excel(folder_path_stations)

stations_list = []
for index, row in df.iterrows():
    # print(row['stationID'])
    station_window = get_window(row['x_coord'], row['y_coord'], row['stationID'])
    stations_list.append(station_window)

merged_stations = xr.concat(stations_list, dim='station')

# Extract variable name (replace with your actual variable name)
variable_name = 'chl_re_gons'

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
    for time_index, time in enumerate(times):
        for lat_index, lat in enumerate(latitudes):
            for lon_index, lon in enumerate(longitudes):
                
                # Extract pixel value
                ##pixel_value = merged_stations[variable_name].isel(y=lat_index, x=lon_index, time=time_index, station=station_index).values.item()
                pixel_value = merged_stations[variable_name].sel(station=station).isel(y=lat_index, x=lon_index, time=time_index).values
                print(pixel_value)

                # Append data to lists
                pixel_values.append(pixel_value)
                coordinates.append((lat, lon, station, time))


df = pd.DataFrame({
    'Latitude': [coord[0] for coord in coordinates],
    'Longitude': [coord[1] for coord in coordinates],
    'Station': [coord[2] for coord in coordinates],
    'Time': [coord[3] for coord in coordinates],
    'Pixel_Value': pixel_values
})

# Save the DataFrame to an Excel file
excel_output_path = 'pixel_windows_value.xlsx' #This miss to integrate extra columns with metadata (e.g. station ID)
df.to_excel(excel_output_path, index=False)