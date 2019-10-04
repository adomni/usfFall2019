import boto3
from credentials import access_key, secret_key, region_name
from time import sleep
import os

client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)


queries = [
"select count(distinct billboard_id) from location_data.hist_20190817_billboard_devices where mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e';",
"select audience from location_data.hist_20190817_device_audiences where mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e'; ",
"""
Select
  billboard_id,
  count(a.mobile_device_id)
from
  location_data.hist_20190817_billboard_devices a
  INNER JOIN (
    select
      mobile_device_id
    FROM
      location_data.hist_20190817_device_audiences
    where
      audience = 'Demographic->Income->200KPlus'
  ) b ON a.mobile_device_id = b.mobile_device_id
group by
  billboard_id
order by
  2 desc
limit
  10
""",
"SELECT count(distinct billboard_id) from location_data.billboard_devices_partitioned where dt=20190817 and mobile_device_id='2a7e1bff-f551-4ee4-a0d9-16d25f99d75e';",
"SELECT mobile_device_id FROM location_data.hist_20190817_billboard_devices where billboard_id = 'f2a8fea85d723bd01600c31a307d1e81'",
"""
SELECT
  distinct billboard_id
from
  location_data.hist_20190817_billboard_devices
WHERE
  mobile_device_id IN (
    SELECT
      mobile_device_id
    FROM
      location_data.hist_20190817_billboard_devices
    where
      billboard_id = 'f2a8fea85d723bd01600c31a307d1e81'
  )
"""
]
 #billboard_audiences, billboard_devices, and device_audiences
query = """
SELECT id, max(count) as max FROM (SELECT a.billboard_id, c.id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20190817) a LEFT JOIN
(SELECT * FROM location_data.device_audiences_partitioned WHERE dt=20190817) b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
location_data.adomni_audience_segment c ON b.audience = c.placeiqid
GROUP BY a.billboard_id, c.id) abc GROUP BY id
"""
 # """SELECT a.billboard_id, c.id, count(distinct a.mobile_device_id) as count FROM
 # location_data.billboard_devices_partitioned a LEFT JOIN
 # location_data.device_audiences_partitioned b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
 # location_data.adomni_audience_segment c ON b.audience = c.placeiqid WHERE a.dt=20190817 and b.dt=20190817
 # GROUP BY billboard_id, audience"""

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
reason = ""
if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
    reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']
if status is not "FAILED":
    os.system("aws s3 cp " + outputLocation + " .")
    os.system("del result.csv")
    os.system("rename " + id + ".csv result.csv")
print(status_query)
print(status)
print(reason)
print(outputLocation)

#for results in response:
        #for row in results['ResultSet']:
            #print(results)
