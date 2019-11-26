# This program is to get the counts of mobile devices for each audience in any billboard. 


import os
import sys
import csv
import boto3
import botocore
from retrying import retry
import pandas as pd
import numpy as np

# configuration
# s3_bucket = 'adomni-placeiq-sync/neon_query_temp'  # S3 Bucket name
s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
s3_output = 's3://' + s3_bucket   # S3 Bucket to store results
database = 'default'  # The database to which the query belongs

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')


@retry(
    # Try it until 5 minutes have passed.
    stop_max_delay = 1000 * 60 * 5,   
    # Sleep 0.2 second. 
    wait_fixed = 200
      )
def poll_status(_id):
    result = athena.get_query_execution(QueryExecutionId=_id)
    state  = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def run_query(audience_placeiqid, query, database, s3_output):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration = {
            'OutputLocation': s3_output,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    result = poll_status(query_execution_id)
    # print(result)
    
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: ")
        print(query_execution_id)
                
        s3_key = query_execution_id + '.csv'
        filename = audience_placeiqid + '.csv'
        
        # Download result file. 
        try:
            s3.Bucket(s3_bucket).download_file(s3_key, 
                'data/counts_for_each_audience/' + filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        
        # Read file to array. 
        # rows = []
        # with open('data/' + filename) as csvfile:
        #     reader = csv.DictReader(csvfile)
        #     for row in reader:
        #         rows.append(row)

        # Delete result file.
#         if os.path.isfile(filename):
#             os.remove(filename)

        # return rows

    if result['QueryExecution']['Status']['State'] == 'FAILED':
        print()
        print("Query FAILED: ")
        print(result['QueryExecution']['Status']['StateChangeReason'])


def make_query(audience_placeiqid):
    
    # query directly raw data. 
    query = """
    SELECT
        a.billboard_id, 
        count(a.mobile_device_id) my_count
    FROM 
        location_data.billboard_devices_partitioned a
    INNER JOIN (
        SELECT 
            mobile_device_id
        FROM 
            location_data.device_audiences_partitioned
        WHERE 
            audience = '%s' 
    ) b 
    ON 
        a.mobile_device_id = b.mobile_device_id
    GROUP BY 
        a.billboard_id
    ORDER BY
        my_count desc
    """ % (audience_placeiqid)

    return query


def query_table(audience_placeiqid):
    # This takes about 2 minutes.
    query = make_query(audience_placeiqid)
    run_query(audience_placeiqid, query, database, s3_output) 


def download_count_for_each_audience(): 

    audience_data = pd.DataFrame(pd.read_csv('data/adomni_audience_segment.csv'))
    audience_placeiqids = audience_data['placeiqid'].values
    total_num_placeiqids = len(audience_placeiqids)
    audience_placeiqids = np.delete(audience_placeiqids, 0)

    i = 1
    for audience_placeiqid in audience_placeiqids:
        print('querying... audience_placeiqid: {0} {1}'.format(audience_placeiqid, str(i) + '/' + str(total_num_placeiqids)))
        query_table(audience_placeiqid)
        i += 1



# Pre-calculate. 
download_count_for_each_audience()


















