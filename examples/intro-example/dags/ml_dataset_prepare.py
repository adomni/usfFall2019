import boto3
from credentials import access_key, secret_key, region_name
from time import sleep
import os
from multiprocessing import Process
from threading import Thread
import pandas as pd


client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)

available_dates = ['20190913', '20190308', '20190513', '20191004', '20191011']

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
        os.system("del result_ " + num + ".csv")
        os.system("rename " + id + ".csv result_" + num + ".csv")
    print(status_query)
    print(status)
    print(reason)
    print(outputLocation)

threads = []
output_filename = 'result.csv'
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
    os.system("del result_" + str(i) + ".csv")


for n,g in combined_df.groupby('id'):
    max = g['max'].max()
    result_df = result_df.append({'id': n, 'max': max}, ignore_index= True)

print(result_df.head())
result_df.to_csv(output_filename, encoding='utf-8', index=False)


#for results in response:
        #for row in results['ResultSet']:
            #print(results)
