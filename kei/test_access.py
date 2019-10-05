import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')


# table = dynamodb.Table('location-audience-2019-08-17')
# print('table name:', table.table_name)
# print(table.creation_date_time)
# print(table.key_schema)



# res = table.get_item(
#     Key={
#         'locationHash': 'b6e71c034fa5e34b5d8a9199208d53cb', 
#         'audienceSegmentId': 2
#     }
# )
# # print(res)
# item = res['Item']

# locationHash = item['locationHash']
# audienceSegmentId = item['audienceSegmentId']
# count = item['count']
# uniqueDevicesAtLocation = item['uniqueDevicesAtLocation']
# dmaIndex = item['dmaIndex']
# dmaName = item['dmaName']
# placeIqId = item['placeIqId']

# print('locationHash:', locationHash)
# print('audienceSegmentId:', audienceSegmentId)
# print('count:', count)
# print('uniqueDevicesAtLocation:', uniqueDevicesAtLocation)
# print('dmaIndex:', dmaIndex)
# print('dmaName:', dmaName)
# print('placeIqId:', placeIqId)



def query_table(table_name, location_hash, audience_segment_id):
    table = dynamodb.Table(table_name)

    response = table.query(
    KeyConditionExpression=Key('locationHash').eq(location_hash) & 
                           Key('audienceSegmentId').eq(audience_segment_id)
    )        

    return response



table_name = 'location-audience-2019-08-17'
location_hash = 'b6e71c034fa5e34b5d8a9199208d53cb'
# audience_segment_ids = [1, 2]
audience_segment_ids = [61, 77]

# res = query_table(table_name, location_hash, audience_segment_id)
# print(res['Items'])

aud_seg_to_count = {}
for audience_segment_id in audience_segment_ids:
    res = query_table(table_name, location_hash, audience_segment_id)
    count = res['Items'][0]['count']
    aud_seg_to_count[audience_segment_id] = count



print(aud_seg_to_count)

# print(items)




































