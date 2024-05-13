import os
import xarray as xr
from datetime import datetime
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import altair as alt

folder_path = '/home/eodc/OUTPUT'  # Replace with the actual path to your folder

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
    new_dataset = xr.concat([dataset["chl_re_gons"]], dim=xr.DataArray(time_series, coords={"time": time_series}, dims=["time"]))
    datasets_list.append(new_dataset)

merged_data = xr.concat(datasets_list, dim='time')

# Specify the 'x' and 'y' coordinates for the time series
station = 'SCHAARVODDL	Schaar van Ouden Doel'
lon = 4.250932603
lat = 51.35118367
#5.8711167e+05
#5.68962179e+06
x_coord =  5.8711167e+05 #5.208e+05  
y_coord =  5.68962179e+06 #5.712e+06
#station = 'VLISSGNLDK	Vlissingen Nolledijk'
#lon = 3.553066,	
#lat = 51.450453
#5.3843131e+05
#5.70006397e+06
#x_coord =  5.3843131e+05   
#y_coord =  5.70006397e+06 

# Extract the time series at the specified 'x' and 'y' coordinates
time_series = merged_data.sel(x=x_coord, y=y_coord, method='nearest')

ch = time_series.values.tolist()
time = time_series.time.values.tolist()
time = [x / 1e9 for x in time]
date_strings = [datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') for timestamp in time]

plt.figure(figsize=(10, 6))
plt.plot(date_strings, ch, marker='o', color='g')
plt.title(f'chl_re_gons\n log10 CHL Gons [mg m-3]\n UTM Easting: {x_coord}, UTM Northing: {y_coord}')
plt.xlabel('Time')
plt.ylabel('log10 CHL Gons [mg m-3]')
plt.grid(True)

output_filename = folder_path + '/plot_ch.png'
print(output_filename)
plt.savefig(output_filename)


data = pd.DataFrame({'Date': date_strings, 'CHL': ch})
y_axis_limits = (0, 10)

# Create an Altair chart
chart = alt.Chart(data.dropna()).mark_bar(size=10).encode(
    x='Date:T',
    y='CHL:Q',
    # scale=alt.Scale(domain=list(y_axis_limits)),
    color=alt.Color(
        'CHL:Q', scale=alt.Scale(scheme='redyellowgreen', domain=(5, 20))),
    tooltip=[
        alt.Tooltip('Date:T', title='Date'),
        alt.Tooltip('log10 CHL Gons [mg m-3]:Q', title='log10 CHL Gons [mg m-3]')
    ]).properties(
    width=600, height=400,
    title=f'chl_re_gons - log10 CHL Gons [mg m-3]\n x_coord: {x_coord}, y_coord: {y_coord}'
)


# Display the Altair chart
print(folder_path + "/plot_ch_atl.html")
chart.save(folder_path + "/plot_ch_atl.html")