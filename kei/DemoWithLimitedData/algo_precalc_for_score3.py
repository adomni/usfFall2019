# This program is to get the counts of high quality mobile devices.  
# Jupyter notebook version of this code is algo_precalc_for_score3.ipynb. 

import numpy as np
import numpy.random as random
import scipy as sp
from pandas import Series, DataFrame
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib as mpl

hq_data = pd.read_csv('hq_data/hq_20190308.csv')

billboard_id1 = '05cc093be9bc7d7a4c491972e235231b' # High score1, Valid for score3
billboard_id2 = '97ee222e0687d37626b2989266640d94' # Low score1, Valid for score3


hq_data_sub = hq_data[(hq_data['billboard_id'] == billboard_id1) | (hq_data['billboard_id'] == billboard_id2)]

hq_data_sub = hq_data_sub[(hq_data_sub['as_one_id'] == 44.0) & 
                          (hq_data_sub['as_two_id'] == 61.0) & 
                          (hq_data_sub['as_three_id'] == 748.0)]

# Get the max count for the given set of audiences. 
hq_data_sub2 = hq_data[(hq_data['as_one_id'] == 44.0) & 
                       (hq_data['as_two_id'] == 61.0) & 
                       (hq_data['as_three_id'] == 748.0)]
max_count = hq_data_sub2['count'].max()
new_data_s = pd.Series(['max', 0, 0, 0, max_count], index=hq_data_sub.columns)
hq_data_sub_with_max = hq_data_sub.append(new_data_s, ignore_index=True)
max_count_df = hq_data_sub_with_max[hq_data_sub_with_max['billboard_id'] == 'max']
hq_data_sub_with_max.to_csv('data/hq_counts_with_max.csv')

























