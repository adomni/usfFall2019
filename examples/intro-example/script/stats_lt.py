import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('input/output.csv')
location = df[['locationHash', 'uniqueDevicesAtLocation']].drop_duplicates()
print(location.head())
df2 = pd.read_csv('input/adomni_device_data9-16-19.csv')
location_type = df2[['locationHash', 'typeName']]
print(location_type.head())
merged_df = pd.merge(location, location_type, on='locationHash')
location_devices = merged_df[['typeName', 'uniqueDevicesAtLocation']]
location_devices['uniqueDevicesAtLocation'] = location_devices['uniqueDevicesAtLocation'].fillna(0.0).astype(int)
print(location_devices.head())

location_devices = location_devices.sort_values(by='typeName')
dict_location_devices = {}

for n,g in location_devices.groupby('typeName'):
    dict_location_devices[n] = g

def remove_outlier(df_in, col_name):
    q1 = df_in[col_name].quantile(0.25)
    q3 = df_in[col_name].quantile(0.75)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_out = df_in.loc[(df_in[col_name] > fence_low) & (df_in[col_name] < fence_high)]
    return df_out

f = open("output/location/output.txt", "w+")
for key, value in dict_location_devices.items():
    print(key, file=f)
    print(value.describe(), file=f)
    print("-----------------------------------", file=f)
    removed_outlier = remove_outlier(value, 'uniqueDevicesAtLocation')
    plt.title("Histogram for Location Type: " + str(key))
    plt.hist(removed_outlier['uniqueDevicesAtLocation'], bins='auto', normed=False)
    plt.ylabel('Frequency');
    plt.savefig("output/location/histogram/location" + str(key).replace('/', ' and ') + ".png", bbox_inches='tight',dpi=100)
    plt.close()


f.close()
