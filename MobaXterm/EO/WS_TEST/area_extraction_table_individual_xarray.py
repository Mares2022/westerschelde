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


# folder_path_l2w_data = r'P:\11209243-eo\Window_extraction\INPUT\L2W'
folder_path_l2w_data = '/eodc/private/deltares/EO/WS/OUTPUT/'
file_path_stations   = '/home/eodc/EO/WS/INPUT/Stations.xlsx'
folder_path_shp      = '/home/eodc/EO/WS/INPUT/polygons_NEOZ.geojson'

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

# List all files in the folder
files_in_folder = os.listdir(folder_path_l2w_data)

# Filter the list to include only files ending with "ic.nc"
filtered_files = [file for file in files_in_folder if file.endswith("L2W.nc")]

# Sort the paths based on the extracted date
sorted_files = sorted(filtered_files, key=extract_date)

# Remove variables that are not used
variables_to_remove = ['transverse_mercator', 'x', 'y', 'lon', 'lat']

# Get variables names in a list. This component is hardcoded and is reading one S2A and one S2B image.
ds_a = xr.open_dataset(os.path.join(folder_path_l2w_data, sorted_files[0])).drop_vars(variables_to_remove)
ds_b = xr.open_dataset(os.path.join(folder_path_l2w_data, sorted_files[-1])).drop_vars(variables_to_remove)

variable_names_A = list(ds_a.variables)
variable_names_B = list(ds_b.variables)
variable_names = list(set(variable_names_A + variable_names_B))

print(variable_names_A)
print(variable_names_B)
print(variable_names)

# Reduce dataset for testing 
sorted_files = sorted_files[:]
variable_names =variable_names[:]

for variable_name in variable_names:
    print('\n'+ variable_name+'\n')
    df = pd.read_excel(file_path_stations)
    stations_list = []
    statistics = []
    df_list = []


    for file in sorted_files:
        path = os.path.join(folder_path_l2w_data, file)
        print(path)

        # Open dataset
        dataset = xr.open_dataset(path)

        #dataset = xr.open_dataset(path, chunks={'x': 100, 'y': 100})
        crs_wkt = dataset.transverse_mercator.attrs['crs_wkt']
        crs = CRS.from_string(crs_wkt)

        dataset.rio.write_crs(crs.to_epsg(), inplace=True)
        dataset = dataset.rio.reproject('EPSG:4326')

        variables_to_remove = ['lon', 'lat']

        dataset =  dataset .drop_vars(variables_to_remove)
        dataset = dataset.rename({'x': 'lon', 'y': 'lat'})

        dataset   = dataset.reset_coords(['transverse_mercator'])
      
        polygons_gdf = gpd.read_file(folder_path_shp)
        polygons_gdf = polygons_gdf.to_crs('EPSG:4326')

        #Assign time component as a variable
        time_series = [datetime.fromisoformat(dataset.attrs.get("isodate"))]  
        ds = xr.concat([dataset[variable_name]], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))

        
        for index, row in polygons_gdf.iterrows():
            ksa_aoi  = polygons_gdf[polygons_gdf.Group == row['Group']]

            # Get coord bounds with buffer for clipping
            ksa_lat = [float(ksa_aoi.total_bounds[1]), float(ksa_aoi.total_bounds[3])]
            ksa_lon = [float(ksa_aoi.total_bounds[0]), float(ksa_aoi.total_bounds[2])]
            lat_multiplier = abs(ksa_lat[1] - ksa_lat[0])
            lon_multiplier = abs(ksa_lon[1] - ksa_lon[0])

            min_lon, max_lon = ksa_lon[0] - (0.1*lon_multiplier), ksa_lon[1] + (0.1*lon_multiplier)
            min_lat, max_lat = ksa_lat[0] - (0.1*lat_multiplier), ksa_lat[1] + (0.1*lat_multiplier)

            # Generate mask
            ksa_mask = regionmask.mask_geopandas(
                ksa_aoi, 
                ds.lon, 
                ds.lat)

            # Clip and mask
            ds_clip = ds.where((ds.lat <= max_lat) & (ds.lat >= min_lat)\
                            & (ds.lon <= max_lon) & (ds.lon >= min_lon), drop=True)
            ds_masked = ds_clip.where(~ksa_mask.isnull())

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
            # print("Mean Value:", mean_value)

            # Plot the masked DataArray
            plt.figure(figsize=(10, 8))
            ds_masked.isel(time=0).plot(cmap='viridis', robust=True)
            
            # Plot the polygons
            # polygons_gdf.plot(ax=plt.gca(), edgecolor='red')

            group = str(row['Group'])
            plt.title(f'Masked {variable_name} with Polygon: {group}')
            # plt.show()
            plt.close()

            statistics.append((time_series, row['Group'], mean_value, median_value, min_value, max_value, std_value, q25 , q75, iqr_value, count_value))

    df = pd.DataFrame({
        'Time': [coord[0][0]for coord in statistics],
        'Group': [coord[1] for coord in statistics],
        variable_name +'_mean': [coord[2].round(decimals=4) for coord in statistics],
        # variable_name +'_median': [coord[3].round(decimals=4) for coord in statistics],
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

    #---------------------------------------------------------------------
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
    excel_output_path = f'/home/eodc/EO/WS/RESULTS/area_extraction_table_{variable_name}.xlsx' 
    merged_df.to_excel(excel_output_path,na_rep='nan')