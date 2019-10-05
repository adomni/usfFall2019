import boto3
from boto3.dynamodb.conditions import Key
dynamodb = boto3.resource('dynamodb')


def query_table(table_name, location_hash, audience_segment_id):
    table = dynamodb.Table(table_name)

    response = table.query(
    KeyConditionExpression=Key('locationHash').eq(location_hash) & 
                           Key('audienceSegmentId').eq(audience_segment_id)
    )        

    return response


def get_count_map(table_name, location_hash, audience_segment_ids):
    aud_seg_to_count = {}
    for audience_segment_id in audience_segment_ids:
        res = query_table(table_name, location_hash, audience_segment_id)
        count = res['Items'][0]['count']
        aud_seg_to_count[audience_segment_id] = count

    return aud_seg_to_count


def get_max_count():
    return 1


def get_normarized_count(aud_seg_to_count, audience_segment_ids):
    for audience_segment_id in audience_segment_ids:
        count = aud_seg_to_count[audience_segment_id]
        max_count = get_max_count() 

        normalized = count / max_count
        aud_seg_to_count[audience_segment_id] = normalized

    return aud_seg_to_count






#precalculate values at night time for maximum?

#using dynamo or athena data for each segmentId

#append the result into a database 

#calculate score
def calculateScore(locationHash, audienceIds):
    score = 0


    #access the precalculated values in the above database to compare the count of locationHash with the precalculated statistic for that audienceId
    table_name = 'location-audience-2019-08-17'
    aud_seg_to_count = get_count_map(table_name, locationHash, audienceIds)
    aud_seg_to_normalized = get_normarized_count(aud_seg_to_count, audienceIds)

    print(aud_seg_to_normalized)

    #score and save the array of all the result for each audienceId and calulate the average



        

#return the result 
    return score


location_hash = 'b6e71c034fa5e34b5d8a9199208d53cb'
audience_segment_ids = [61, 77]
calculateScore(location_hash, audience_segment_ids)























