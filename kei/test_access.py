import boto3
dynamodb = boto3.resource('dynamodb')


table = dynamodb.Table('location-audience-2019-08-17')
print('table name:', table.table_name)
# print(table.creation_date_time)
# print(table.key_schema)



res = table.get_item(
    Key={
        'locationHash': 'b6e71c034fa5e34b5d8a9199208d53cb', 
        'audienceSegmentId': 1
    }
)
# print(res)
item = res['Item']

locationHash = item['locationHash']
audienceSegmentId = item['audienceSegmentId']
count = item['count']
uniqueDevicesAtLocation = item['uniqueDevicesAtLocation']
dmaIndex = item['dmaIndex']
dmaName = item['dmaName']
placeIqId = item['placeIqId']

print('locationHash:', locationHash)
print('audienceSegmentId:', audienceSegmentId)
print('count:', count)
print('uniqueDevicesAtLocation:', uniqueDevicesAtLocation)
print('dmaIndex:', dmaIndex)
print('dmaName:', dmaName)
print('placeIqId:', placeIqId)







# Display all the tables. 
# table_list = dynamodb.tables.all()
# for table in table_list:
#     print(table.table_name)



# print(table.table_name)
# print(table.creation_date_time)


