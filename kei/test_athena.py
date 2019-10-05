#!/usr/bin/env python
# coding: utf-8

# In[4]:


import boto3
athena = boto3.client('athena')

# query = 'select * from location_data.hist_20190817_billboard_devices where mobile_device_id=\'{LOCATION_HASH_PREFIX}\';';
query = 'select *                from location_data.hist_20190817_billboard_devices                where mobile_device_id=2a7e1bff-f551-4ee4-a0d9-16d25f99d75e;';

athena.start_query_execution(
    QueryString = query, 
    QueryExecutionContext = {
        'Database': 'default'
    },
    ResultConfiguration = {
        'OutputLocation': 's3://adomni-placeiq-sync/neon_query_temp',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3'
        }
    }
)


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




