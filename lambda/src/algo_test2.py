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
import numpy as np
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
#from tuo_count_predictor import get_predicted_count

dynamodb = boto3.resource('dynamodb')
table_count = dynamodb.Table('usf-location-audience-2019-11-08')
table_hq = dynamodb.Table('high_quality')
table_predict = dynamodb.Table('PredictedCount')

# configuration
# s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
# s3_output = 's3://' + s3_bucket   # S3 Bucket to store results
# database = 'default'  # The database to which the query belongs

# init clients
# athena = boto3.client('athena')
# s3 = boto3.resource('s3')


# Get all audience_ids.
all_audience_ids = []
def get_all_audience_ids():
    aud_data = pd.read_csv('data/adomni_audience_segment.csv')
    all_aud_ids = aud_data['id']
    all_aud_ids = all_aud_ids.drop(0)
    all_aud_ids = np.array(all_aud_ids)
    return all_aud_ids


# Get the count of mobile devices for the billboard_id and the placeiqid.
def get_count(billboard_id, audience_id):
    response = table_count.query(KeyConditionExpression=Key('locationHash').eq(billboard_id) & Key('audienceSegmentId').eq(int(audience_id)))
    if len(response['Items']) > 0:
        count = str(response['Items'][0]['count'])
        print(billboard_id + ' ' + str(audience_id))
        print("Count: " + count)
        return count
    else:
        #print("Count: 0")
        return 0
    #audience_data = pd.DataFrame(pd.read_csv('data/counts_for_each_audience/' + placeiqid + '.csv'))
    #response = audience_data[audience_data['billboard_id'] == billboard_id]['my_count'].values

    #return response
def get_predicted_count(billboard_audience_segment_id):
    response = table_predict.query(KeyConditionExpression=Key('billboard_audience_segment_id').eq(billboard_audience_segment_id))
    if len(response['Items']) > 0:
        count = str(response['Items'][0]['predicted_count'])
        print(billboard_audience_segment_id)
        print("Count: " + count)
        return count
    else:
        #print("Count: 0")
        return 0


# Make count map that maps audience_id to count of mobile devices for that billboard_id.
def get_count_map(billboard_id, audience_ids):
    # aud_seg_to_count (Key: audience_segment_id, Value: count)
    aud_seg_to_count = {}
    audience_seg_data = pd.DataFrame(pd.read_csv('data/adomni_audience_segment.csv'))

    for audience_id in audience_ids:
        # Get placeiqid corresponding to the audience_id.
        audience_id = str(audience_id)
        placeiqid = audience_seg_data[audience_seg_data['id'] == audience_id]['placeiqid'].values
        print('input audience id: {0} {1}'.format(audience_id, placeiqid))
        #res = get_count(billboard_id, placeiqid[0])
        res = get_count(billboard_id, audience_id)
        count = res
        aud_seg_to_count[audience_id] = count

    return aud_seg_to_count

def get_timeseries(billboard_id, audience_ids):
    aud_seg_to_count = {}
    for audience_id in audience_ids:
        billboard_audience_segment_id = str(billboard_id) + str(audience_id)
        count = get_predicted_count(billboard_audience_segment_id)
        aud_seg_to_count[str(audience_id)] = count

    return aud_seg_to_count

# Get the maximum count for each audience_id to normalize the count.
def get_max_count(audience_id):

    max_counts = pd.read_csv('data/result_max.csv')
    max_count = max_counts[max_counts['id'] == audience_id]['max']

    return max_count.values[0]


# Get the normalized count for each audience_id.
def get_normalized_count(aud_seg_to_count, audience_ids):
    for audience_id in audience_ids:
        audience_id = str(audience_id)
        count = aud_seg_to_count[audience_id]
        max_count = get_max_count(int(audience_id))

        normalized = int(count) / max_count
        aud_seg_to_count[audience_id] = normalized

    return aud_seg_to_count


# Get the average.
def get_average(segId_by_normalizedScore):
    integratedAdomniScore = 0
    for segId in segId_by_normalizedScore:
        integratedAdomniScore = integratedAdomniScore + segId_by_normalizedScore[segId]
    return integratedAdomniScore/len(segId_by_normalizedScore)


# Normalized score based on the count of mobile devices for given audiences.
def get_score1(billboard_id, audience_ids, timeseries):

    # aud_seg_to_count (Key: audience_id, Value: count)
    if timeseries:
        print("timeseries on")
        aud_seg_to_count = get_timeseries(billboard_id, audience_ids)
    else:
        print("timeseries off")
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
    #uniqueDevicesAtLocation
    df = pd.read_csv('data/count_for_each_billboard_with_max.csv')
    count = df[df['billboard_id'] == billboard_id]['count'].values[0]
    # print('count:', count)
    max_count2 = df[df['billboard_id'] == 'max']['count'].values[0]
    # print('max:', max_count2)
    score2 = int(count) / int(max_count2)

    return score2


# Normalized score based on the count of high quality mobile devices.
def get_score3(billboard_id, age, gender, other_aud):
    audience_id = age + '_' + gender + '_' + other_aud
    response = table_hq.query(KeyConditionExpression=Key('billboard_id').eq(billboard_id) & Key('audience_segment_id').eq(audience_id))
    if len(response['Items']) > 0:
        count = str(response['Items'][0]['count'])
        #print("Count: " + count)
    else:
        #print("Count: 0")
        return 0
    # df = pd.read_csv('data/hq_counts_with_max.csv')
    # count = df[(df['billboard_id'] == billboard_id) & (df['audin'] == billboard_id)]['count'].values[0]
    # # print('count:', count)
    # max_count3 = df[df['billboard_id'] == 'max']['count'].values[0]
    # # print('max:', max_count3)
    # score3 = int(count) / int(max_count3)
    max = 100
    score3 = int(count)/max
    return score3


# Normalized score based on the clusters that are captured by K-Means Clustering.
def get_score4(billboard_id, audience_ids):
    score4 = 0.0

    # Read files that K-means clustering created.
    billboard_with_cluster_only = pd.read_csv('data/billboard_with_cluster_only.csv')
    normalized_score = pd.read_csv('data/norm_scores_for_each_cluster.csv')

    # Get the cluster that the billboard belongs to.
    cluster = billboard_with_cluster_only[billboard_with_cluster_only['billboard_id'] == billboard_id]['cluster'].values[0]

    # Get the average of the normalized scores.
    for aud_id in audience_ids:
        aud_id = str(aud_id)
        aud_id = 'a' + aud_id
        score = normalized_score.loc[cluster, aud_id]
        # print('score:', score)
        score4 += score

    score4 = score4 / len(audience_ids)

    return score4


# Calculate Adomni Score.
def calculate_score(billboard_id, audience_ids, algorithm):
    response = {}
    #default algorithm - no Kmeans and no timeseries prediction
    if algorithm == "DEFAULT":
        timeseries = False
        kmeans = False

    #algorithm 2 - use timeseries but no kmeans
    elif algorithm == "KEI":
        timeseries = False
        kmeans = True
    elif algorithm == "TUO":
        timeseries = True
        kmeans = False
    elif algorithm == "BOTH":
        timeseries = True
        kmeans = True
    else:
        print("invalid algorithm")
        response['adomni_score'] = -1
        return response

    response['timeseries'] = timeseries
    response['kmeans'] = kmeans

    adomni_score = 0.0
    scores = []

    # Get score1: Normalized score based on the count of mobile devices for the given audiences.
    score1 = get_score1(billboard_id, audience_ids, timeseries)
    scores = np.append(scores, score1)
    response['score1'] = score1
    print('score1:', score1)

    # Get score2: Normalized score based on the count of mobile devices for any audience.
    score2 = get_score2(billboard_id)
    scores = np.append(scores, score2)
    response['score2'] = score2
    print('score2:', score2)

    # Get score3: Normalized score based on the count of high quality mobile devices.
    if len(audience_ids) >= 3:
        count_score3 = 0
        score3 = 0
        age_list = []
        gender_list = []
        other_aud_list = []
        for audience_id in audience_ids:
            audience_id = str(audience_id)
            if audience_id in ['39','40','41','42','43','44','45','46','47']:
                age_list.append(audience_id)
            elif audience_id == '60' or audience_id == '61':
                gender_list.append(audience_id)
            elif audience_id in all_audience_ids:
                other_aud_list.append(audience_id)
        for age in age_list:
            for gender in gender_list:
                for other_aud in other_aud_list:
                    temp_score3 = get_score3(billboard_id, age, gender, other_aud)
                    score3 = score3 + temp_score3
                    count_score3 = count_score3 + 1
        score3 = score3/count_score3
        if score3 != 0:
            scores = np.append(scores, score3)
            print('score3:', score3)
            response['score3'] = score3
        else:
            print('score3: not enough parameters')




    # Get score4: Normalized score based on the clusters that are captured by K-Means Clustering.
    if kmeans:
        score4 = get_score4(billboard_id, audience_ids)
        scores = np.append(scores, score4)
        print('score4:', score4)
        response['score4'] = score4
        W1 = 0.25
        W2 = 0.25
        W3 = 0.25
        W4 = 0.25
        print('Weight for score1: ' + str(W1))
        print('Weight for score2: ' + str(W2))
        print('Weight for score3: ' + str(W3))
        print('Weight for score4: ' + str(W4))
        adomni_score = (score1 * W1) + (score2 * W2) + (score3 * W3) + (score4 * W4)
    else:
        print('score4: turned off' )
        W1 = 1/3
        W2 = 1/3
        W3 = 1/3
        print('Weight for score1: ' + str(W1))
        print('Weight for score2: ' + str(W2))
        print('Weight for score3: ' + str(W3))
        adomni_score = (score1 * W1) + (score2 * W2) + (score3 * W3)


    # for score in scores:
    #     adomni_score += score
    #
    # adomni_score = adomni_score / len(scores);
    response['adomni_score'] = adomni_score
    return response






#####################
# Start here.
#####################

# Weights.
# W1: Weight for normalized score based on the count of mobile devices for given audiences.
# W2: Weight for normalized score based on the count of mobile devices for any audience.
# W3: Weight for normalized score based on the count of high quality mobile devices.
# W4: Weight for normalized score based on the clusters that are captured by K-Means Clustering.


# Test cases
# billboard_id = 'dbb561c792f78028f262e88ce95f857c' # Valid for score3
# billboard_id = '05cc093be9bc7d7a4c491972e235231b' # High score1, Valid for score3
# billboard_id = '03c393dbf2ae41661307a19457ea2e89' # Valid for score3
# billboard_id = '36e0958762ec4fda133545176d7176b9' # Invalid for score4
# billboard_id = '65d9eef54ad59f641c651b961666657c'
# billboard_id = '50158cf1c6fded24e3b510d0d6dbd8e3' # Low score1, Invalid for score3
# billboard_id = '97ee222e0687d37626b2989266640d94' # Low score1, Valid for score3
# billboard_id = 'f41ac46de8c208f6cf64fef66255f0eb'
# billboard_id = '42c5508521fdd113e63172ccd256b74e' # Low score1
# billboard_id = '6943fd9adccae67d803b28dd8e33a0b3' # Low score1


all_audience_ids = get_all_audience_ids()


# # Demographic->Age->35_44
# # Demographic->Gender->Male
# # AutomotiveDealerships->Luxury
audience_ids = [44, 61, 748]
# print()
#
# test 1
#05cc093be9bc7d7a4c491972e235231b
billboard_id = 'bestbillboardexample' # high
print('input billboard id:', billboard_id)
adomni_score = calculate_score(billboard_id, audience_ids, "BOTH")
print('---------------------------------------')
print('Adomni Score:', adomni_score)
print('---------------------------------------')
print()

# test 2
billboard_id = 'worstbillboardexample' # low
print('input billboard id:', billboard_id)
adomni_score = calculate_score(billboard_id, audience_ids, "BOTH")
print('---------------------------------------')
print('Adomni Score:', adomni_score)
print('---------------------------------------')
print()































#aws s3 cp s3://result-ouput/result_max.csv ./data/


# billboard_ids -> random

# print out the placeiqids as well.

#



# audience_ids = ['748', '738']
