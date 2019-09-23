#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import numpy.random as random
import scipy as sp
import pandas as pd
from pandas import Series, DataFrame

import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')

get_ipython().run_line_magic('precision', '3')


# In[2]:


billboards_data = pd.read_csv('data/billboards_20190916.csv')
billboards_data.head()


# In[3]:


len(billboards_data)


# In[4]:


# multiple rows for each locationHash


# In[15]:


billboards_data.groupby('typeName').size()
# multiple rows for each typeName


# In[5]:


def isOneToOne(df, col1, col2):
    first = df.groupby(col1)[col2].count().max()
    second = df.groupby(col2)[col1].count().max()
    return first + second == 2

isOneToOne(billboards_data, 'locationHash', 'typeName')


# In[6]:


location_audience_data = data = pd.read_csv('data/output.csv', error_bad_lines=False)
location_audience_data.head()


# In[7]:


len(location_audience_data)


# In[8]:


location_audience_data.groupby('locationHash').size()
# multiple rows for each locationHash. 


# In[9]:


location_audience_data[location_audience_data['locationHash']=="fecaa6e724c8d218682f00e49328a173"]


# In[10]:


isOneToOne(location_audience_data, 'locationHash', 'uniqueDevicesAtLocation')


# In[23]:


locH_to_uniqueDevicesAtLocation = location_audience_data[['locationHash','uniqueDevicesAtLocation']]
locH_to_uniqueDevicesAtLocation


# In[25]:


locH_to_uniqueDevicesAtLocation = locH_to_uniqueDevicesAtLocation.drop_duplicates()
locH_to_uniqueDevicesAtLocation


# In[28]:


# inner join
df_merged = pd.merge(locH_to_uniqueDevicesAtLocation, billboards_data, on='locationHash')
df_merged.head(10)


# In[30]:


# NG!? sum()??
grouped_by_typeName_and_locH = df_merged.groupby(['typeName','typeId','locationHash'])['uniqueDevicesAtLocation'].sum().unstack()
print(type(grouped_by_typeName_and_locH))
grouped_by_typeName_and_locH
# Since the location_audience_data is partial data, some of the typeName are deleted when inner joined. 


# In[21]:


f = open("output_kei/location/output.txt", "w+")
for index, row in grouped_by_typeName_and_locH.iterrows():
    print(index[0], file=f)
    row = row.dropna()
    print(row.describe(), file=f)
    print("-----------------------------------", file=f)
#     removed_outlier = remove_outlier(value, 'count')
    plt.title("Histogram for Location Type: " + index[0])
    plt.hist(row, bins='auto')
    plt.xlabel('uniqueDevicesAtLocation')
    plt.ylabel('Frequency');
    plt.grid(True)
    plt.savefig("output_kei/location/histogram/location_type_" + str(int(index[1])) + ".png", bbox_inches='tight',dpi=100)
    plt.close()


f.close()
print('done.')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




