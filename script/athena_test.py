import boto3
from credentials import access_key, secret_key, region_name
from time import sleep
import os

client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)


queries = [
"select count(distinct billboard_id) from location_data.hist_20190817_billboard_devices where mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e';",
"select audience from location_data.hist_20190817_device_audiences where mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e'; "
"Select billboard_id, count(mobile_device_id) from location_data.hist_20190817_billboard_devices a \
INNER JOIN (select mobile_device_id FROM location_data.hist_20190817_device_audiences where audience_id = 'Demographic->Income->200KPlus') b \
ON a.mobile_device_id = b.mobile_device_id group by billboard_id order by 2 desc limit 10",
"select count(distinct billboard_id) from billboard_devices_partitioned where dt=20190817 and mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e';"
]
 #billboard_audiences, billboard_devices, and device_audiences
query = "select count(distinct billboard_id) from location_data.billboard_devices_partitioned where dt=20190817 and mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e';"
response = client.start_query_execution(
    QueryString = query,
    QueryExecutionContext = {'Database': 'default'},
    ResultConfiguration = {
        'OutputLocation': 's3://adomni-placeiq-sync/neon_query_temp',
        'EncryptionConfiguration': {
        'EncryptionOption': 'SSE_S3'
        }
    }
)

id = response['QueryExecutionId']

while (client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] == 'RUNNING'):
    print("Waiting...")
    sleep(5)
status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']
os.system("aws s3 cp " + outputLocation + " .")

os.system("rename " + id + ".csv result.csv")
print(status_query)
print(status)
print(outputLocation)

#for results in response:
        #for row in results['ResultSet']:
            #print(results)
