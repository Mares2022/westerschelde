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

def make_plots(x_coord, y_coord, file_name_png, file_name_html):
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

    output_filename = folder_path + file_name_png
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
    print(folder_path + file_name_html)
    chart.save(folder_path + file_name_html)

# Specify the 'x' and 'y' coordinates for the time series
station = 'SCHAARVODDL	Schaar van Ouden Doel'
lon = 4.250932603
lat = 51.35118367
x_coord =  5.8711167e+05 
y_coord =  5.68962179e+06 

file_name_png  = '/plot_ch_SCHAARVODDL.png'
file_name_html = f"/plot_ch_atl_SCHAARVODDL.html"

make_plots(x_coord, y_coord, file_name_png, file_name_html)

station = 'VLISSGNLDK	Vlissingen Nolledijk'
lon = 3.553066	
lat = 51.450453
x_coord =  5.3843131e+05   
y_coord =  5.70006397e+06

file_name_png  = '/plot_ch_VLISSGNLDK.png'
file_name_html = f"/plot_ch_atl_VLISSGNLDK.html"

make_plots(x_coord, y_coord, file_name_png, file_name_html)

station = 'TERNZSDSGDW	Terneuzen, scheldesteiger bij DOW'
lon = 3.788685156
lat = 51.35129967
x_coord =  5.5492237e+05   
y_coord =  5.68918718e+06 

station = 'HANSWGL	Hansweert geul'
lon = 4.014388534	
lat = 51.43701537
x_coord =  5.7050772e+05   
y_coord =  5.69891249e+06 