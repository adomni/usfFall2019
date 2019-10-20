


#precalculate values at night time for maximum?

#using dynamo or athena data for each segmentId

#append the result into a database 

#calculate score

import os
import sys
import csv
import boto3
import botocore
from retrying import retry
import pandas as pd

# configuration
# s3_bucket = 'adomni-placeiq-sync/neon_query_temp'  # S3 Bucket name
s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
s3_output = 's3://' + s3_bucket   # S3 Bucket to store results
database = 'default'  # The database to which the query belongs

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')


def get_count(billboard_id, placeiqid):
    audience_data = pd.DataFrame(pd.read_csv('data/counts_for_each_audience/' + placeiqid + '.csv'))
    response = audience_data[audience_data['billboard_id'] == billboard_id]['my_count'].values    

    return response


def get_count_map(billboard_id, audience_ids):
    # aud_seg_to_count (Key: audience_segment_id, Value: count)
    aud_seg_to_count = {}
    audience_seg_data = pd.DataFrame(pd.read_csv('data/adomni_audience_segment.csv'))

    for audience_id in audience_ids:
        placeiqid = audience_seg_data[audience_seg_data['id'] == audience_id]['placeiqid'].values
        res = get_count(billboard_id, placeiqid[0])
        count = res[0]
        aud_seg_to_count[audience_id] = count

    return aud_seg_to_count


def get_max_count(audience_id):
    # Take as input the result_max.csv here.
    max_counts = pd.read_csv('data/result_max.csv')
    max_count = max_counts[max_counts['id'] == audience_id]['max']

    return max_count.values[0]


def get_normalized_count(aud_seg_to_count, audience_ids):
    for audience_id in audience_ids:
        count = aud_seg_to_count[audience_id]
        max_count = get_max_count(int(audience_id)) 

        normalized = int(count) / max_count
        aud_seg_to_count[audience_id] = normalized

    return aud_seg_to_count


#precalculate values at night time for maximum?
#using dynamo or athena data for each segmentId
#append the result into a database 

#calculate score
def calculate_score(billboard_id, audience_ids):
    score = 0


    #access the precalculated values in the above database to compare the count of locationHash with the precalculated statistic for that audienceId

    # aud_seg_to_count (Key: audience_id, Value: count)
    aud_seg_to_count = get_count_map(billboard_id, audience_ids)

    # aud_seg_to_normalized (Key: audience_segment_id, Value: normalized count)
    aud_seg_to_normalized = get_normalized_count(aud_seg_to_count, audience_ids)

    print('aud_seg_to_normalized: \n{}'.format(aud_seg_to_normalized))

    #score and save the array of all the result for each audienceId and calulate the average



        

    #return the result 
    return aud_seg_to_normalized

#segId_by_normalizedScore should be a map like {("b6e71c034fa5e34b5d8a9199208d53cb", 76): 0.3, ("b6e71c034fa5e34b5d8a9199208d53cb", 176): 0.85, ("b6e71c034fa5e34b5d8a9199208d53cb", 85): 0.4}
def getIntegratedAdomniScore(segId_by_normalizedScore):
    integratedAdomniScore = 0
    for segId in segId_by_normalizedScore:
        integratedAdomniScore = integratedAdomniScore + segId_by_normalizedScore[segId]
    return integratedAdomniScore/len(segId_by_normalizedScore)


segId_by_normalizedScore = {}
billboard_id = 'dbb561c792f78028f262e88ce95f857c'
audience_ids = ['748', '738']
aud_seg_to_normalized = calculate_score(billboard_id, audience_ids)

for audience_segment_id in aud_seg_to_normalized:
    segId_by_normalizedScore[(billboard_id, audience_segment_id)] = aud_seg_to_normalized[audience_segment_id]

print(getIntegratedAdomniScore(segId_by_normalizedScore))






