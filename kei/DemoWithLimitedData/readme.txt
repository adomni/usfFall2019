

---------------------------------------
algo_precalc_for_score1.py

This program is to get the counts of mobile devices for each audience in any billboard. 
This takes long long time. Tae has a better solution. 

inputs: 
Tables in the S3 bucket. 

outputs: 
Csv files for each audience, which contains all billboard ids and count. 

---------------------------------------
algo_precalc_for_score2.py

This program is to get the counts of mobile devices for any audience.  
And also, append the max count to the file. 

inputs: 
Tables in the S3 bucket. 

outputs: 
data/count_for_each_billboard_with_max.csv

---------------------------------------
algo_precalc_for_score3.py

This program is to get the counts of high quality mobile devices. 
For now, this just creates small data from 17 GB hq_20190308.csv for our demo.    

inputs:
s3://result-output/high_quality/hq_20190308.csv

outputs:
data/hq_counts_with_max.csv

---------------------------------------
algo_precalc_for_score4.py

This program is to train the k-means clustering model and get the cluster for each billboard. 
And also, get the normalized scores for each cluster.

inputs: 
data/input_kmc.csv 

outputs: 
data/billboard_with_cluster_only.csv
data/norm_scores_for_each_cluster.csv

---------------------------------------
algo_test2.py

This program needs to return the Adomni Score immediately when called. 

inputs:
(for score1 and score3) data/adomni_audience_segment.csv 
(for score1) data/counts_for_each_audience/AutomotiveDealerships->Luxury.csv 
(for score1) data/counts_for_each_audience/Demographic->Gender->Male.csv 
(for score1) data/counts_for_each_audience/Demographic->Age->35_44.csv 
(for score1) data/result_max.csv
(for score2) data/count_for_each_billboard_with_max.csv
(for score3) data/hq_counts_with_max.csv
(for score4) data/billboard_with_cluster_only.csv
(for score4) data/norm_scores_for_each_cluster.csv


outputs:
Adomni Score

























