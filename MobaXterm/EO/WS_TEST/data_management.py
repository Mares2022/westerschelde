import zipfile
import shutil
import os
import pandas as pd
import geopandas as gpd

# csv_file_path = r'c:\Users\fuentesm\CISNE\Deltares\Westershelde\INPUT\Sentinel-2-collection-to-feature-31UES.csv'
csv_file_path_with_geometry = '/home/eodc/EO/WS/INPUT/Sentinel-2-collection-to-feature-31UET_30_percentage_cloud_coverage.csv'

# df = pd.read_csv(csv_file_path, header=0)  # Set header=0 to use the first row as columns
gdf = gpd.read_file(csv_file_path_with_geometry)

#Example format image name
#S2A_MSIL1C_20230823T104631_N0509_R051_T31UET_20230823T143001.zip

products = pd.DataFrame(gdf['PRODUCT_ID'])
#products_S2A_OPER_PRD_MSIL1C_PDMC = products[products['PRODUCT_ID'].str.startswith('S2A_OPER_PRD_MSIL1C_PDMC')].reset_index(drop=True)
#products_S2A_MSIL1C = products[products['PRODUCT_ID'].str.startswith('S2A_MSIL1C')].reset_index(drop=True)
#products_S2B_MSIL1C = products[products['PRODUCT_ID'].str.startswith('S2B_MSIL1C')].reset_index(drop=True)

products['orbit']  = products['PRODUCT_ID'].str.split('_').str.get(0)
products['date']   = products['PRODUCT_ID'].str.split('_').str.get(-1)
products['year']   = products['date'].str[:4]
products['month']  = products['date'].str[4:6]
products['day']    = products['date'].str[6:8]
products['path']   = products['PRODUCT_ID'] + '.zip'

years_to_filter  = ['2015','2016','2017','2018','2019','2020','2021','2022','2023','2024']
products         = products[products['year'].isin(years_to_filter)]
year_list        = products['year'].tolist()[0:5]
month_list       = products['month'].tolist()[0:5]
day_list         = products['day'].tolist()[0:5]
product_list     = products['PRODUCT_ID'].tolist()[0:5]

# Get the directory of the current Python script
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, 'INPUT')

products_dir = '/eodc/products'
move_to_dir  = os.path.join(script_dir, 'INPUT')

# Ensure the output and move-to directory exists; create it if it doesn't
if not os.path.exists(input_dir):
    os.makedirs(input_dir)
if not os.path.exists(move_to_dir):
    os.makedirs(move_to_dir)

path_list = []

for index, row in products[:].iterrows():
    
    if row["orbit"] == 'S2A':
        subfolders = ["copernicus.eu", "s2a_prd_msil1c", row["year"], row["month"], row["day"], row["path"]]
    else:
        subfolders = ["copernicus.eu", "s2b_prd_msil1c", row["year"], row["month"], row["day"], row["path"]]
    zip_file_path = os.path.join(products_dir, *subfolders)

    print(f'Index: {index}, year: {row["year"]}, month: {row["month"]}')
    print(zip_file_path)
    print(os.path.join(move_to_dir, os.path.basename(zip_file_path)))

    path_list.append(zip_file_path)



# Specify the path to the output text file
output_file_path = os.path.join('/home/eodc/EO/WS/INPUT/', 'path_list_30_percentage.txt')

#zip_file_path = os.path.join(script_dir, 'path_list.txt')

# Open the file in write mode
with open(output_file_path, "w") as output_file:
    # Iterate through the list and write each item to the file
    for item in path_list:
        output_file.write(item + "\n")

print("List has been saved to", output_file_path)