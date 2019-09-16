from credentials import access_key, secret_key, region_name
import boto3
from boto3 import dynamodb

from boto3.session import Session
from boto3.dynamodb.conditions import Key

dynamodb_session = Session(aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)

dynamodb = dynamodb_session.resource('dynamodb')
table = dynamodb.Table('location-audience-2019-08-17')


location_hashs = ["ac2959cc7ac7466bd91b08e58e5490d2", "ac0e4be8494c4c34a4c8c4e1ca47253c", "aeafeac6a78c548227f37bdd32cfb516", "eb54b7e048c7353be69954ea451d0276", "3c8da0f4e53ecd2e20fb620962bda2f3", "ea2c659d3b5fead1b7ced561722e9216"]
segment_ids = [[61],[755],[77],[61,755,77]]
def calculateAdomniScore(table, locationHash, segment_ids):
    for segment_id in segment_ids:
        string_id = map(str, segment_id)
        list_string = ', '.join(string_id)
        print("For segment id: " + list_string)
        for id in segment_id:
            response = table.query(KeyConditionExpression=Key('locationHash').eq(locationHash) & Key('audienceSegmentId').eq(id))
            if len(response['Items']) > 0:
                count = str(response['Items'][0]['count'])
                print("Count: " + count)
            else:
                print("Count: 0")
                return 0
    return 1

for location_hash in location_hashs:
    print("For locatin hash: " + location_hash)
    calculateAdomniScore(table, location_hash, segment_ids)
    print("-----------------------------------------------------------------------------------")
