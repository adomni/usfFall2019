import boto3
from credentials import access_key, secret_key, region_name
from time import sleep
import os
from multiprocessing import Process
from threading import Thread
import pandas as pd


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

 # """SELECT a.billboard_id, c.id, count(distinct a.mobile_device_id) as count FROM
 # location_data.billboard_devices_partitioned a LEFT JOIN
 # location_data.device_audiences_partitioned b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
 # location_data.adomni_audience_segment c ON b.audience = c.placeiqid WHERE a.dt=20190817 and b.dt=20190817
 # GROUP BY billboard_id, audience"""
#a to f

def runQuery(num):

    query = """
    SELECT id, max(count) as max FROM (SELECT a.billboard_id, c.id, count(distinct a.mobile_device_id) as count FROM
    (SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20190913 and billboard_id like '""" + num + """%') a LEFT JOIN
    (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=20190913) b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
    location_data.adomni_audience_segment c ON b.audience = c.placeiqid
    GROUP BY a.billboard_id, c.id) abc GROUP BY id ORDER BY id asc
    """

    response = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {'Database': 'default'},
        ResultConfiguration = {
            'OutputLocation': 's3://athena-output-usf',
            'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3'
            }
        }
    )
    id = response['QueryExecutionId']

    while (client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] == 'RUNNING'):
        print("Waiting... "  + num)
        sleep(5)
    status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
    reason = ""
    if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
        reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
    status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
    outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']
    if status is not "FAILED":
        os.system("aws s3 cp " + outputLocation + " .")
        os.system("rm result_ " + num + ".csv")
        os.system("mv " + id + ".csv result_" + num + ".csv")
    print(status_query)
    print(status)
    print(reason)
    print(outputLocation)

threads = []
output_filename = 'result_max.csv'
for i in range (0, 10):
    t = Thread(target=runQuery, args=(str(i)))
    threads.append(t)
    t.start()

for x in threads:
    x.join()

header = ['id', 'max']

combined_df = pd.DataFrame(columns=header)
result_df = pd.DataFrame(columns=header)
for i in range (0, 10):
    temp_df = pd.read_csv("result_" + str(i) + ".csv")
    combined_df = combined_df.merge(temp_df, how='outer')
    os.system("rm result_" + str(i) + ".csv")


for n,g in combined_df.groupby('id'):
    max = g['max'].max()
    result_df = result_df.append({'id': n, 'max': max}, ignore_index= True)

print(result_df.head())
result_df.to_csv(output_filename, encoding='utf-8', index=False)

os.system("aws s3 cp " + output_filename + " s3://result-output/")

#for results in response:
        #for row in results['ResultSet']:
            #print(results)
