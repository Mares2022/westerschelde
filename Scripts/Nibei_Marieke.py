
"""
Created on Thu Feb 29 16:43:13 2024
@authors: eleveld 
For testing in the Westerschelde
For more info on the Switch and Nibei alorithms marieke.eleveld@deltares.nl
"""

# This snippet of code assumes Acolite surface reflectances (rhos) output in "dataset"
# please replace by your own favourite

## Nibei to differentiate between optically shallow (OS) and deep waters (OD)
## Source / plse refer to: Arabi B, Salama MS, van der Wal D, Pitarch J, Verhoef W, 2020. RS Env. 237, 111596
def get_nibei(dataset):
    # c=np.log10(dataset.rhos_739)
    c=np.log10(dataset.rhos_740)
    # d=np.log10(dataset.rhos_864)
    d=np.log10(dataset.rhos_865)
    nibeidiv=c/d #nibeidiv<1 is OD, >1= OS
    # dataset['nibei'] = nibei
    # dataset.nibei.attrs['unit'] = '-'
    return nibeidiv

## This is one suggestion of how it could be implemented. We can also start by only adding it to the Excel file first,
## to the maps later. Or if you do not manage we can post-process it in the Excel. No problem!
##  Masks have over time been updated. They are now ..
## flag_exponent_swir=0 Bit index that will be used to store the SWIR threshold masking step (2**0).
## flag_exponent_cirrus=1 Bit index that will be used to store the cirrus masking step (2**1).
## flag_exponent_toa=2 Bit index that will be used to store the top-of-atmosphere threshold masking step (2**2).
## flag_exponent_negative=3 Bit index that will be used to store the negative retrievals masking step (2**3).
## flag_exponent_outofscene=4 Bit index that will be used to store the out-of-scene masking step (2**4).

def mask_all_var(dataset, nibeidiv): 
    main_filename = filename.split('L2R.nc')
   
    l2w = r'..L2W\\{}{}'.format(main_filename[0], 'L2W.nc') #adapt path and filename settings
    ds_l2w = xr.open_dataset(l2w) 
    ds_l2w.close()
    flag = ds_l2w.l2_flags
    
    for i in list(dataset.variables):
        if i == 'lat':
            continue
        elif i == 'lon':
            continue
        # elif i == 'rd_y':
        #     continue
        # elif i == 'rd_x':
        #     continue
        else:
            mask = np.ma.masked_where(((flag==1) & (nibeidiv>1)), dataset[i]) #0=water, 1 &2 =land #nibeidiv indicated Optically Deep or Shallow waters
            # mask = np.ma.masked_where((flag==1), dataset[i]) #0=water, 1 &2 =land
            masked_data = xr.DataArray(mask)
            # dataset[i] = (('y', 'x'), masked_data)
            dataset[i] = masked_data
    
    return dataset
