#####################################################################
# Calculate Adomni Score given the billboard_id and the audience_ids. 
# Version: 2.0.0
# Author: Tae, Tuo, and Kei
# Note  : This needs some other programs for pre-calculation. 
#####################################################################

# import os
# import sys
# import csv
# import boto3
# import botocore
import pandas as pd

# configuration
# s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
# s3_output = 's3://' + s3_bucket   # S3 Bucket to store results
# database = 'default'  # The database to which the query belongs

# init clients
# athena = boto3.client('athena')
# s3 = boto3.resource('s3')


# Get the count of mobile devices for the billboard_id and the placeiqid. 
def get_count(billboard_id, placeiqid):
    audience_data = pd.DataFrame(pd.read_csv('data/counts_for_each_audience/' + placeiqid + '.csv'))
    response = audience_data[audience_data['billboard_id'] == billboard_id]['my_count'].values    

    return response


# Make count map that maps audience_id to count of mobile devices for that billboard_id. 
def get_count_map(billboard_id, audience_ids):
    # aud_seg_to_count (Key: audience_segment_id, Value: count)
    aud_seg_to_count = {}
    audience_seg_data = pd.DataFrame(pd.read_csv('data/adomni_audience_segment.csv'))

    for audience_id in audience_ids:
        placeiqid = audience_seg_data[audience_seg_data['id'] == audience_id]['placeiqid'].values
        print('input audience id: {0} {1}'.format(audience_id, placeiqid))
        res = get_count(billboard_id, placeiqid[0])
        count = res[0]
        aud_seg_to_count[audience_id] = count

    return aud_seg_to_count


# Access the precalculated values in the above database to compare the count of mobile devices with the precalculated statistic for that audience_id. 
# For now, use maximum count to normalize the count. 
def get_max_count(audience_id):
    
    max_counts = pd.read_csv('data/result_max.csv')
    max_count = max_counts[max_counts['id'] == audience_id]['max']

    return max_count.values[0]


# Get the normalized count for each audience_id. 
def get_normalized_count(aud_seg_to_count, audience_ids):
    for audience_id in audience_ids:
        count = aud_seg_to_count[audience_id]
        max_count = get_max_count(int(audience_id)) 

        normalized = int(count) / max_count
        aud_seg_to_count[audience_id] = normalized

    return aud_seg_to_count


#segId_by_normalizedScore should be a map like {("b6e71c034fa5e34b5d8a9199208d53cb", 76): 0.3, ("b6e71c034fa5e34b5d8a9199208d53cb", 176): 0.85, ("b6e71c034fa5e34b5d8a9199208d53cb", 85): 0.4}
def get_average(segId_by_normalizedScore):
    integratedAdomniScore = 0
    for segId in segId_by_normalizedScore:
        integratedAdomniScore = integratedAdomniScore + segId_by_normalizedScore[segId]
    return integratedAdomniScore/len(segId_by_normalizedScore)


# Normalized score based on the count of mobile devices for given audiences. 
def get_score1(billboard_id, audience_ids):

    # aud_seg_to_count (Key: audience_id, Value: count)
    aud_seg_to_count = get_count_map(billboard_id, audience_ids)

    # aud_seg_to_normalized (Key: audience_segment_id, Value: normalized count)
    aud_seg_to_normalized = get_normalized_count(aud_seg_to_count, audience_ids)

    print('Normalized counts: ')
    for k, v in aud_seg_to_normalized.items():
        print(k, ': ', v)

    # score and save the array of all the result for each audienceId and calulate the average
    segId_by_normalizedScore = {}
    for audience_segment_id in aud_seg_to_normalized:
        segId_by_normalizedScore[(billboard_id, audience_segment_id)] = aud_seg_to_normalized[audience_segment_id]

    score1 = get_average(segId_by_normalizedScore)

    return score1


# Normalized score based on the count of mobile devices for any audience. 
def get_score2(billboard_id):
    df = pd.read_csv('data/count_for_each_billboard.csv')
    count = df[df['billboard_id'] == billboard_id]['count'].values[0]
    # print('count:', count)
    max_count2 = df[df['billboard_id'] == 'max']['count'].values[0]
    # print('max:', max_count2)
    score2 = int(count) / int(max_count2)  
    return score2


# Normalized score based on the count of high quality mobile devices. 
def get_score3():

    # TODO: 

    return 0


# Normalized score based on the location type. 
def get_score4():

    # TODO: 

    return 0



# Calculate Adomni Score.  
def calculate_score(billboard_id, audience_ids):
    adomni_score = 0.0
    score1 = 0.0
    score2 = 0.0
    score3 = 0.0
    score4 = 0.0

    # Normalized score based on the count of mobile devices for given audiences. 
    score1 = get_score1(billboard_id, audience_ids)
    print('score1:', score1)

    # Normalized score based on the count of mobile devices for any audience. 
    score2 = get_score2(billboard_id)
    print('score2:', score2)

    # Normalized score based on the count of high quality mobile devices. 
    score3 = get_score3()
    print('score3:', score3)

    # Normalized score based on the location type. 
    score4 = get_score4()
    print('score4:', score4)
    

    adomni_score = (score1 * W1) + (score2 * W2) + (score3 * W3) + (score4 * W4);
        
    return adomni_score








#####################
# Start here. 
#####################

# Weights.
# W1: Weight for normalized score based on the count of mobile devices for given audiences. 
# W2: Weight for normalized score based on the count of mobile devices for any audience. 
# W3: Weight for normalized score based on the count of high quality mobile devices. 
# W4: Weight for normalized score based on the location type correlated well with the high counts for specific audiences. 
W1 = 0.25
W2 = 0.25
W3 = 0.25
W4 = 0.25

# Test cases
# billboard_id = 'dbb561c792f78028f262e88ce95f857c'
# billboard_id = '05cc093be9bc7d7a4c491972e235231b' # high
# billboard_id = '03c393dbf2ae41661307a19457ea2e89'
# billboard_id = '36e0958762ec4fda133545176d7176b9'
# billboard_id = '65d9eef54ad59f641c651b961666657c'
# billboard_id = '50158cf1c6fded24e3b510d0d6dbd8e3' # low
# billboard_id = '97ee222e0687d37626b2989266640d94'
# billboard_id = 'f41ac46de8c208f6cf64fef66255f0eb'
# billboard_id = '42c5508521fdd113e63172ccd256b74e'
# billboard_id = '6943fd9adccae67d803b28dd8e33a0b3'


# Demographic->Age->35_44
# Demographic->Gender->Male
# AutomotiveDealerships->Luxury
audience_ids = ['44', '61', '748']
print()

# test 1 high score
billboard_id = '05cc093be9bc7d7a4c491972e235231b' # high
print('input billboard id:', billboard_id)
adomni_score = calculate_score(billboard_id, audience_ids)
print('---------------------------------------')
print('Adomni Score:', adomni_score)
print('---------------------------------------')
print()

# test 2 low score
billboard_id = '50158cf1c6fded24e3b510d0d6dbd8e3' # low
print('input billboard id:', billboard_id)
adomni_score = calculate_score(billboard_id, audience_ids)
print('---------------------------------------')
print('Adomni Score:', adomni_score)
print('---------------------------------------')
print()































#aws s3 cp s3://result-ouput/result_max.csv ./data/


# billboard_ids -> random 

# print out the placeiqids as well. 

# 



# audience_ids = ['748', '738']





















