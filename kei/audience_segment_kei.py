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


location_audience_data = data = pd.read_csv('data/output.csv', error_bad_lines=False)
location_audience_data.head()


# In[4]:


location_audience_data.groupby('audienceSegmentId').size()
# There seems to be multiple rows for each audienceSegmentId. 


# In[5]:


location_audience_data.groupby('locationHash').size()
# There seems to be multiple rows for each locationHash. 


# In[6]:


location_audience_data.describe()


# In[7]:


as1 = location_audience_data[location_audience_data['audienceSegmentId']==1]
as1.head(20)


# In[8]:


as1.groupby('locationHash').size()


# In[9]:


s = as1.groupby('locationHash')['count'].sum()
print(type(s))
s


# In[10]:


s.describe()


# In[11]:


plt.hist(s, bins='auto')
key = 1
plt.title("Histogram for Audience Segment: " + str(int(key)))
plt.xlabel('mobile device counts')
plt.ylabel('Frequency')
plt.grid(True)


# In[12]:


dict_audience = {}
dict_audience[1] = s


# In[13]:


grouped_by_as_and_lH = location_audience_data.groupby(['audienceSegmentId','locationHash'])['count'].sum().unstack()
print(type(grouped_by_as_and_lH))
grouped_by_as_and_lH


# In[14]:


x = grouped_by_as_and_lH.loc[1]
x


# In[15]:


type(x)


# In[16]:


x = x.dropna()


# In[17]:


plt.hist(x, bins='auto')
key = 1
plt.title("Histogram for Audience Segment: " + str(int(key)))
plt.xlabel('mobile device counts')
plt.ylabel('Frequency')
plt.grid(True)


# In[18]:


num_rows = len(grouped_by_as_and_lH)
num_rows


# In[19]:


x2 = grouped_by_as_and_lH.loc[832]
x2


# In[20]:


# for index, row in grouped_by_as_and_lH.iterrows():
#     print(type(index))
#     print(index)
#     print('~~~~~~')

#     print(type(row))
#     print(row)
#     row = row.dropna()
#     print(row.describe())
#     print('------')
#     print('--------------------------------------')


# In[21]:


f = open("output_kei/audience_segment/output.txt", "w+")
for index, row in grouped_by_as_and_lH.iterrows():
    print(index, file=f)
    row = row.dropna()
    print(row.describe(), file=f)
    print("-----------------------------------", file=f)
#     removed_outlier = remove_outlier(value, 'count')
    plt.title("Histogram for Audience Segment: " + str(int(index)))
    plt.hist(row, bins='auto')
    plt.xlabel('mobile device counts')
    plt.ylabel('Frequency');
    plt.grid(True)
    plt.savefig("output_kei/audience_segment/histogram/audience_segment_" + str(int(index)) + ".png", bbox_inches='tight',dpi=100)
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




