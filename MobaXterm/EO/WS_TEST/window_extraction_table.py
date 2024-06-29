import xarray as xr
from pyproj import CRS
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, box, LineString
import folium
import os
from datetime import datetime
import matplotlib.pyplot as plt
import altair as alt
import numpy as np
import re
import psutil
import sys
import regionmask
from pyproj import CRS
import dask.array as da

def create_map(gdf):
    # Get the center of the map based on the points
    center_lat, center_lon = gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()

    # Create a Folium map centered at the mean coordinates
    mymap = folium.Map(location=[center_lat, center_lon], zoom_start=10)

    return mymap


def add_points_to_map(gdf, mymap):

    # Add GeoPoints to the map
    # for idx, row in gdf.iterrows():
    #    folium.Marker(
    #        [row.geometry.y, row.geometry.x], 
    #        popup=f"Point {idx}"
    #    ).add_to(mymap)

    # Add Circle markers to the map
    for idx, row in gdf.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=5,  # Adjust the radius as needed
            color='blue',  # Circle color
            fill=True,
            fill_color='blue',  # Fill color
            fill_opacity=0.6,
            popup=f"Point {idx}"
        ).add_to(mymap)

    return mymap

def add_boundingbox_to_map(mymap):
    # Define limits in Westerschelde
    # limit=51.2,3.3,51.55,4.4

    # Create a bounding box from the given limits
    min_lat, min_lon, max_lat, max_lon = 51.2, 3.1, 51.55, 4.4
    bbox = box(min_lon, min_lat, max_lon, max_lat)

    # Create a GeoDataFrame with the bounding box and set the CRS
    gdf_bbox = gpd.GeoDataFrame(geometry=[bbox], crs='EPSG:4326')

    # Add GeoDataFrame to the map
    folium.GeoJson(gdf_bbox).add_to(mymap)

    # Display the map
    return mymap 

# Define a function to extract the date from the path. Data about the year.
def extract_date(path):

    match = re.search(r'(\d{4}_\d{2}_\d{2})', path)
    if match:
        return match.group()
    else:
        return "0000_00_00"  # Return a default date if no date is found
    
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

# Define function to get the coordinates in the netcdfs closer to the stations coordinates.
def get_exact_coordinates_in_netcdf(dataset, x_coord, y_coord):

    time_series   = dataset.sel(x=x_coord, y=y_coord, method='nearest')
    y_coord_exact = time_series['y'].values.tolist()
    x_coord_exact = time_series['x'].values.tolist()

    return y_coord_exact, x_coord_exact 

def extract_surrounding_coordinates(ds, x_coord, y_coord):

    # Find the indices of the closest pixels
    x_index = np.abs(ds['x'] - x_coord).argmin().item()
    y_index = np.abs(ds['y'] - y_coord).argmin().item()
    
    # Extract the coordinates of the surrounding pixels
    x_surrounding = ds['x'].values[x_index-1:x_index+2]
    y_surrounding = ds['y'].values[y_index-1:y_index+2]
    
    # Create a meshgrid for the surrounding pixels
    xx, yy = np.meshgrid(x_surrounding, y_surrounding)
    
    # Flatten the meshgrid to get coordinates
    x_surrounding = xx.flatten()
    y_surrounding = yy.flatten()
    
    return x_surrounding, y_surrounding

folder_path_l2w_data = '/eodc/private/deltares/EO/WS/OUTPUT/'
file_path_stations   = '/home/eodc/EO/WS/INPUT/Stations.xlsx'
sorted_files, variable_names = get_files_and_variable_names(folder_path_l2w_data)
df_stations = pd.read_excel(file_path_stations)
sorted_files = sorted_files[:]

sorted_files[0]

file_path_output_window_geojson = '/home/eodc/EO/WS/INPUT/window_extraction.geojson' 
file_path = os.path.join(folder_path_l2w_data, sorted_files[0])
ds        = xr.open_dataset(file_path) 

coordinates  = []

for index, row in df_stations.iterrows():
    y_coord_exact, x_coord_exact  = get_exact_coordinates_in_netcdf(ds, row['x_coord'], row['y_coord'])
    coordinates.append((row['stationID'], row['station'], row['lon'], row['lat'], row['x_coord'], row['y_coord'], x_coord_exact, y_coord_exact))

df_exact_coordinates = pd.DataFrame({
    'stationID': [coord[0] for coord in coordinates],
    'station': [coord[1] for coord in coordinates],
    'lon': [coord[2] for coord in coordinates],
    'lat': [coord[3] for coord in coordinates],
    'x_coord': [coord[4] for coord in coordinates],
    'y_coord': [coord[5] for coord in coordinates],
    'x_coord_exact': [coord[6] for coord in coordinates],
    'y_coord_exact': [coord[7] for coord in coordinates],
    'x_coord_1': [coord[0] for coord in coordinates],
    'y_coord_1': [coord[0] for coord in coordinates],
    'x_coord_2': [coord[0] for coord in coordinates],
    'y_coord_2': [coord[0] for coord in coordinates],
    'x_coord_3': [coord[0] for coord in coordinates],
    'y_coord_3': [coord[0] for coord in coordinates],
    'x_coord_4': [coord[0] for coord in coordinates],
    'y_coord_4': [coord[0] for coord in coordinates],
    'x_coord_5': [coord[0] for coord in coordinates],
    'y_coord_5': [coord[0] for coord in coordinates],
    'x_coord_6': [coord[0] for coord in coordinates],
    'y_coord_6': [coord[0] for coord in coordinates],
    'x_coord_7': [coord[0] for coord in coordinates],
    'y_coord_7': [coord[0] for coord in coordinates],
    'x_coord_8': [coord[0] for coord in coordinates],
    'y_coord_8': [coord[0] for coord in coordinates],
    'x_coord_9': [coord[0] for coord in coordinates],
    'y_coord_9': [coord[0] for coord in coordinates],
})  


for index, row in df_exact_coordinates.iterrows():

    x_surrounding, y_surrounding = extract_surrounding_coordinates(ds, row['x_coord'], row['y_coord'])
    print(row['stationID'], row['station'])

    df_exact_coordinates.at[index, 'x_coord_1'] = x_surrounding[0]
    df_exact_coordinates.at[index, 'y_coord_1'] = y_surrounding[0]
    df_exact_coordinates.at[index, 'x_coord_2'] = x_surrounding[1]
    df_exact_coordinates.at[index, 'y_coord_2'] = y_surrounding[1]
    df_exact_coordinates.at[index, 'x_coord_3'] = x_surrounding[2]
    df_exact_coordinates.at[index, 'y_coord_3'] = y_surrounding[2]
    df_exact_coordinates.at[index, 'x_coord_4'] = x_surrounding[3]
    df_exact_coordinates.at[index, 'y_coord_4'] = y_surrounding[3]
    df_exact_coordinates.at[index, 'x_coord_5'] = x_surrounding[4]
    df_exact_coordinates.at[index, 'y_coord_5'] = y_surrounding[4]
    df_exact_coordinates.at[index, 'x_coord_6'] = x_surrounding[5]
    df_exact_coordinates.at[index, 'y_coord_6'] = y_surrounding[5]
    df_exact_coordinates.at[index, 'x_coord_7'] = x_surrounding[6]
    df_exact_coordinates.at[index, 'y_coord_7'] = y_surrounding[6]
    df_exact_coordinates.at[index, 'x_coord_8'] = x_surrounding[7]
    df_exact_coordinates.at[index, 'y_coord_8'] = y_surrounding[7]
    df_exact_coordinates.at[index, 'x_coord_9'] = x_surrounding[8]
    df_exact_coordinates.at[index, 'y_coord_9'] = y_surrounding[8]

    print(f"Surrounding pixel coordinates: {list(zip(x_surrounding, y_surrounding))}")

df = df_exact_coordinates 
# Create geometry objects (Points) from X1, Y1, X2, and Y2 columns
zipline =  zip(df['x_coord_1'], df['y_coord_1'], df['x_coord_2'], df['y_coord_2'], df['x_coord_3'], df['y_coord_3'], df['x_coord_4'], df['y_coord_4'], df['x_coord_5'], df['y_coord_5'], df['x_coord_6'], df['y_coord_6'], df['x_coord_7'], df['y_coord_7'], df['x_coord_8'], df['y_coord_8'], df['x_coord_9'], df['y_coord_9'])
geometry = [LineString([(x1, y1), (x2, y2), (x3, y3), (x4, y4), (x5, y5), (x6, y6), (x7, y7), (x8, y8), (x9, y9)]) for x1, y1, x2, y2, x3, y3, x4, y4,x5, y5, x6, y6,x7, y7, x8, y8, x9, y9 in zipline]

# Create a GeoDataFrame	
gdf_window = gpd.GeoDataFrame(df, geometry=geometry, crs='32631')
gdf_window = gdf_window.to_crs('EPSG:4326')

# Save  GeoDataFrame	
gdf_window.to_file(file_path_output_window_geojson, driver='GeoJSON')

list_subsets = [[0,1],[1,2]]

for subset in list_subsets:

  ds = xr.open_dataset(f'/home/eodc/EO/WS/RESULTS/window_extraction_output_TUR_Nechad2016_865.nc')
  ds
  
  # file_path = r'D:\Projects\Westerschelde\OUTPUT\window_extraction_output_rhow_442_rhow_443.nc'
  # ds = xr.open_dataset(file_path)
  # ds
  
  #variable_names = ['l2_flags', 'chl_re_gons']
  # variable_names = ['rhow_442', 'rhow_443']
  variable_names = list(ds.data_vars)
  print(variable_names)
  window_list = []
  
  for var in variable_names: 
  
      ds_teration = ds[var]
  
      for index, row in df.iterrows():
  
          ds_filtered_1 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_1'], y=row['y_coord_1'], method='nearest')
          ds_filtered_2 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_2'], y=row['y_coord_2'], method='nearest')
          ds_filtered_3 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_3'], y=row['y_coord_3'], method='nearest')
          ds_filtered_4 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_4'], y=row['y_coord_4'], method='nearest')
          ds_filtered_5 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_5'], y=row['y_coord_5'], method='nearest')
          ds_filtered_6 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_6'], y=row['y_coord_6'], method='nearest')
          ds_filtered_7 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_7'], y=row['y_coord_7'], method='nearest')
          ds_filtered_8 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_8'], y=row['y_coord_8'], method='nearest')
          ds_filtered_9 = ds_teration.isel(station=row['stationID']).sel(x=row['x_coord_9'], y=row['y_coord_9'], method='nearest')
  
          data = {
              'Date':   ds_filtered_1.time.values.tolist(),
              'Station': row['stationID'],
              var+'_1': ds_filtered_1.values.tolist(),
              var+'_2': ds_filtered_2.values.tolist(),
              var+'_3': ds_filtered_3.values.tolist(),
              var+'_4': ds_filtered_4.values.tolist(),
              var+'_5': ds_filtered_5.values.tolist(),
              var+'_6': ds_filtered_6.values.tolist(),
              var+'_7': ds_filtered_7.values.tolist(),
              var+'_8': ds_filtered_8.values.tolist(),
              var+'_9': ds_filtered_9.values.tolist(),
          }
  
  
          if 'df_window' not in locals() and 'df_window' not in globals():
              # If 'df_window' does not exist, do something
              print("DataFrame 'df_window' does not exist.")
              df_window = pd.DataFrame(data)
              df_window['Date'] = pd.to_datetime(df_window['Date'])
  
          else:
              # If 'df_window' exists, concatenate the variable to itself
              print("DataFrame 'df_window' exists.")
              df_window_concat = pd.DataFrame(data)
              df_window_concat['Date'] = pd.to_datetime(df_window_concat['Date'])
  
              df_window = pd.concat([df_window, df_window_concat], axis=0, ignore_index=True)
  
      window_list.append(df_window)
      
      if 'df_window_all_vars' not in locals() and 'df_window_all_vars' not in globals():
          print("DataFrame 'df_window_all_vars' does not exist.")
          df_window_all_vars = df_window.copy()
  
      else:
          print("DataFrame 'df_window_all_vars' exists.")
          df_window_all_vars = pd.merge(df_window_all_vars, df_window.drop(columns=['Date','Station']), left_index=True, right_index=True, how='inner')
  
      del df_window 
  
  df_window_all_vars.to_excel(f'/home/eodc/EO/WS/RESULTS/window_extraction_table_TUR_Nechad2016_865.xlsx', sheet_name='Sheet1', index=False)
  del df_window_all_vars
