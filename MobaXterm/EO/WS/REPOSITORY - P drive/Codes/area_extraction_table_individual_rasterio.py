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
#import cf_xarray
from pyproj import CRS
import dask.array as da

import os
import dask
import xarray as xr
import pyproj
import rioxarray
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape


def print_memory():
    # Get memory information
    memory_info = psutil.virtual_memory()
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

    window_values = dataset.isel(y=slice(lat_start, lat_end), x=slice(lon_start, lon_end))
    window_values = window_values.expand_dims(station=[stationID])
    
    return window_values

def clip_raster(rds, geometry):
    rds_clipped = rds.rio.clip(geometry)
    return rds_clipped

def clip_raster_with_gdf(rds, gdf):
    rds_clipped_list = []
    for index, row in gdf.iterrows():
        aoi = row["Group"]
        try:
            geometry = gdf.iloc[index:index+1].geometry
            rds_clipped = rds.rio.clip(geometry)
            rds_clipped_list.append(rds_clipped)
            print(f"Successful processing row {index} {aoi}")
        except Exception as e:
            print(f"Error processing row {index} {aoi}: {e}")
            rds_clipped_list.append("NaN")
            continue
    print("\n")
    return rds_clipped_list 

# folder_path_l2w_data = r'P:\11209243-eo\Window_extraction\INPUT\L2W'
folder_path_l2w_data = '/eodc/private/deltares/OUTPUT_10m_2015_2024/'
folder_path_shp      = '/home/eodc/EO/WS_PRODUCTION/INPUT/polygons_NEOZ.geojson'
excel_output_path = '/home/eodc/EO/WS_PRODUCTION/RESULTS/'

# List all files in the folder
files_in_folder = os.listdir(folder_path_l2w_data )
filtered_files = [file for file in files_in_folder if file.endswith("L2W.nc")]
sorted_files = sorted(filtered_files, key=extract_date)

# Remove variables that are not used
variables_to_remove = ['transverse_mercator', 'x', 'y', 'lon', 'lat']

# Get variables names in a list. This component is hardcoded and is reading one S2A and one S2B image.
ds_a = xr.open_dataset(os.path.join(folder_path_l2w_data, sorted_files[0])).drop_vars(variables_to_remove)
ds_b = xr.open_dataset(os.path.join(folder_path_l2w_data, sorted_files[-1])).drop_vars(variables_to_remove)

variable_names_A = list(ds_a.variables)
variable_names_B = list(ds_b.variables)
variable_names = sorted(list(set(variable_names_A + variable_names_B)), key=str.lower)

print(variable_names_A)
print(variable_names_B)
print(variable_names)

# Reduce dataset for testing 
sorted_files = sorted_files[:]
variable_names =variable_names[3:]

polygons_gdf = gpd.read_file(folder_path_shp)
polygons_gdf = polygons_gdf.to_crs('EPSG:32631')

for variable_name in variable_names:

    print('\n'+ variable_name+'\n')

    df_list = []
    ds_list = []
    stations_list = []
    statistics = []

    for file in sorted_files:
        path = os.path.join(folder_path_l2w_data, file)
        print(path)
        
        try:
          # rds = rioxarray.open_rasterio(path, chunks={'y': 100, 'x':100})
          rds = rioxarray.open_rasterio(path)
          time_series = [datetime.fromisoformat(rds.attrs.get("isodate"))]
          rds_var = rds[variable_name]
  
          for index, row in polygons_gdf.iterrows():
              aoi = row["Group"]
              try:
                  geometry = polygons_gdf.iloc[index:index+1].geometry
                  rds_nodata  = rds_var.where((rds_var != 9.96921e+36) & (rds_var != -2147483647), np.nan)
                  rds_nodata  = rds_nodata.rio.write_crs('EPSG:32631')
                  ds_masked  = rds_nodata.rio.clip(geometry, all_touched=False)
                  ds_masked  = ds_masked.where((ds_masked != 9.96921e+36) & (ds_masked != -2147483647), np.nan)
  
                  # Calculate the mean value for the selected area
                  mean_value = ds_masked.mean().values
                  median_value = ds_masked.median().values
                  min_value = ds_masked.min().values
                  max_value = ds_masked.max().values
                  std_value = ds_masked.std().values
                  q25 = ds_masked.quantile(0.25).values
                  q75 = ds_masked.quantile(0.75).values
                  iqr_value = q75 - q25
                  count_value = ds_masked.count().values
                  statistics.append((time_series, row['Group'], mean_value, median_value, min_value, max_value, std_value, q25 , q75, iqr_value, count_value))
                  print("Mean Value:", mean_value)
                  # rds_clipped_list.append(rds_clipped)
                  print(f"Successful processing row {index} {aoi}")
              except Exception as e:
                  print(f"Error processing row {index} {aoi}: {e}")
                  # rds_clipped_list.append("NaN")
                  continue

        except Exception as e:
            print(f"The file {file}: {e} does not have the current variable. It will be skipped")
            continue  

    df = pd.DataFrame({
        'Time': [coord[0][0]for coord in statistics],
        'Group': [coord[1] for coord in statistics],
        variable_name +'_mean': [coord[2].round(decimals=4) for coord in statistics],
        variable_name +'_median': [coord[3].round(decimals=4) for coord in statistics],
        variable_name +'_min': [coord[4].round(decimals=4) for coord in statistics],
        variable_name +'_max': [coord[5].round(decimals=4) for coord in statistics],
        variable_name +'_std': [coord[6].round(decimals=4) for coord in statistics],
        variable_name +'_q25': [coord[7].round(decimals=4) for coord in statistics],
        variable_name +'_q75': [coord[8].round(decimals=4) for coord in statistics],
        variable_name +'_iqr': [coord[9].round(decimals=4) for coord in statistics],
        variable_name +'_count': [coord[10] for coord in statistics],
    })

    df['ID'] =  df['Time'].dt.strftime('%Y_%m_%d') + '_' + df['Group'].round().astype(int).astype(str) 

    # df.set_index('Time', inplace=True)

    df_list.append(df)

    # Create a single table 
    merged_df = df_list[0].iloc[:,2:]  

    for df in df_list[1:]:
        merged_df = pd.merge(merged_df, df.iloc[:,2:], on='ID', how='outer')

    split_columns = merged_df['ID'].str.split('_', expand=True)
    merged_df.insert(0, 'Year', split_columns[0].astype(str))
    merged_df.insert(1, 'Month', split_columns[1].astype(str))
    merged_df.insert(2, 'Day', split_columns[2].astype(str))
    merged_df.insert(3, 'Group', split_columns[3].astype(str))

    merged_df.insert(4, 'Date', pd.to_datetime(merged_df[['Year', 'Month', 'Day']], errors='coerce').dt.date)

    merged_df = merged_df.replace('nan', np.nan)
    merged_df.set_index('ID', inplace=True)

    merged_df.to_excel(excel_output_path + f'area_extraction_table_rio_{variable_name}.xlsx',na_rep='nan')
