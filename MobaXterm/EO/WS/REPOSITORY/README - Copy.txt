
TOOLS

####data_management.py

Input: 
The code requires as input a .csv file with a list of S2L1C images created through: 
https://code.earthengine.google.com/1ed5b07328cc56cab5e2090b8074e61d

Output:
This code creates a list of products available in /eodc/products repository as a.txt file.

####make_animation.py

Input:
The code requires the .png images of the atmospheric corrected images produced with acolite. It will read authomatically all the images 
inside the OUTPUT* folder that is selected.

Ouput:
Animation in .gif format of all the available images.

Note:
There are different make_animation.py codes. Each one of them will create an animation for L1C, L2A, and products (chlorophyll a) images.


####window_extraction_netcdf.py

Input:
It requires a OUTPUT* folder with the L2W products, defining the variables to save in the netcdf file, and Stations.xlsx file defining the stations where to extract the data from ('/eodc/private/deltares/Stations.xlsx')

Output:
This code creates a netcdf with data for the coordinates defined in the Stations.xlsx, all the available images in the L2W folder, and all the variables defined.
process_files_linux.sh


###window_extraction_table_individual.py

Input:
It requires the output netcdf files derived with the code window_extraction_netcdf.py. The code flats the netcdf data to a table.

Output:
It create excel/csv tables with information of the 9-window pixel per station as window_extraction_table_*.xlsx tables. These tables can be found in the OUTPUT folder.

###window_extraction_table_individual.py

Input:
This code requires the geometries where to extract data from as AOI/polygons_NIOZ.geojson file, and the netcdf files generated with the window_extraction_netcdf.py.

Output:
The code provides a data with regional statistics of each geometry provided in the AOI/polygons_NIOZ.geojson file as area_extraction_table_rio_*.xlsx files. These tables can be found in the OUTPUT folder.


#### process_files_linux.sh

INPUT:
This shell code runs acolite software in a linux machine using as input a configuration file from acolite and a list of 
images saved in INPUT folder.

OUTPUT:
The shell script saved acolite results in the selected output folder. 

Note:
There are different process_files scripts. Each of them will save the output in local or private folders or will work in a windows machine (.bat).

This workflow considers the integration of two tiles and works exclusively for the Westerschelde region (T31UET/T31UES)

