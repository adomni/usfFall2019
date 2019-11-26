# K-Means Clustering. 
# Jupyter notebook version of this code is cluster/kmc_all_data.ipynb. 

import numpy as np
import numpy.random as random
import scipy as sp
from pandas import Series, DataFrame
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns

import sklearn
from sklearn.cluster import KMeans


NUMBER_OF_CLUSTERS = 5

# Read input file.
print('Reading input file...')
billboard_data = pd.read_csv('data/input_kmc.csv')
s = billboard_data[billboard_data['lat'] == 'Bar/Restaurant Jukebox']
billboard_data = billboard_data.drop(billboard_data.index[[54025]])

# Change types. 
billboard_data[['lat']] = billboard_data[['lat']].astype(float)
billboard_data[['typeId']] = billboard_data[['typeId']].astype(str)

# Remove some columns. 
billboard_data_loc_sub = billboard_data[['billboard_id','lat','lng','typeId']]
billboard_data_aud_sub = billboard_data.iloc[:,10:]

# Concatenate those sub data. 
training_data = pd.concat([billboard_data_loc_sub, billboard_data_aud_sub], axis=1)

# Remove some rows where typeId is equal to 0.0 or nan. 
training_data = training_data[training_data['typeId'] != '0.0']
training_data = training_data[training_data['typeId'] != 'nan']
training_data = training_data.fillna(0)

# Create the training data. 
training_data_iddropped = training_data.drop('billboard_id', axis=1)

# Standardization

# Elbow curve to find the best number of clusters. Takes about 15 minutes. 

# cost_list = []
# for i in range(1, 10): 
#     kmeans = KMeans(n_clusters=i, init='random', random_state=0)
#     kmeans.fit(training_data_iddropped)
#     cost_list.append(kmeans.inertia_)

# plt.plot(range(1,10), cost_list, marker='+')
# plt.xlabel('Number of clusters')
# plt.ylabel('Cost')


# Learn the data. 
print('Learning dataset...')
kmeans = KMeans(n_clusters=NUMBER_OF_CLUSTERS)
kmeans.fit(training_data_iddropped)

# Concatenate billboards and predicted clusters.  
cluster_predicted = kmeans.predict(training_data_iddropped)
training_data_iddropped['cluster'] = cluster_predicted
billboard_with_cluster = pd.concat([training_data['billboard_id'], training_data_iddropped], axis=1)

# Convert type decimal number string to int in 'typeId'. ("75.0" => 75)
billboard_with_cluster['typeId'] = billboard_with_cluster['typeId'].astype(float)
billboard_with_cluster['typeId'] = billboard_with_cluster['typeId'].astype(np.int64)

# Write out the output cluster. 
print('Creating billboard_with_cluster_only.csv...')
billboard_with_cluster_only = pd.concat([training_data['billboard_id'], training_data_iddropped['cluster']], axis=1)
billboard_with_cluster_only.to_csv('data/billboard_with_cluster_only.csv')

# Score each cluster based on the median count for given aud. 
aud_with_cluster_sub = billboard_with_cluster.drop(['billboard_id','lat','lng','typeId'], axis=1)
median_for_each_aud_and_cluster = aud_with_cluster_sub.groupby('cluster').median()
max_s = median_for_each_aud_and_cluster.max(axis=1)
max_a = np.array(max_s)
normalized_score = median_for_each_aud_and_cluster.divide(max_a, axis=0)

# Write out the normalized scores for each cluster. 
print('Creating norm_scores_for_each_cluster.csv...')
normalized_score.to_csv('data/norm_scores_for_each_cluster.csv')

print('done.')






























