# This program is to get the counts of mobile devices for any audience.  
# And also, append the max count to the file. 

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
# s3_bucket = 'aws-athena-query-results-734644148268-us-east-1/00-clustering-test' # Invalid bucket name
s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
s3_output = 's3://' + s3_bucket   # S3 Bucket to store results
database = 'default'  # The database to which the query belongs

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')


@retry(
    # Try it until 90 minutes have passed.
    stop_max_delay = 1000 * 60 * 90,   
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


def run_query(date, query, database, s3_output):
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
        # filename = 'count_for_each_billboard_' + date + '.csv'
        filename = 'count_for_each_billboard.csv'
        
        # Download result file. 
        try:
            print("downloading... ")            
            s3.Bucket(s3_bucket).download_file(s3_key, 
                'data/' + filename)

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


def make_query(date):
    
    # query directly raw data. 
    # TODO: How do I get 20191108 data? 
    query = """
    SELECT
        billboard_id, 
        count(mobile_device_id) count
    FROM 
        location_data.billboard_devices_partitioned
    GROUP BY
        billboard_id
    """ 


    return query


def append_max():
    count_for_each_bill = pd.read_csv('data/count_for_each_billboard.csv')
    max_count_for_any_aud = count_for_each_bill['count'].max()
    new_data_s = pd.Series(['max',max_count_for_any_aud], index=count_for_each_bill.columns)
    count_for_each_bill = count_for_each_bill.append(new_data_s, ignore_index=True)
    count_for_each_bill.to_csv('data/count_for_each_billboard_with_max.csv')


def query_table(date):
    # This takes about 2 minutes.
    query = make_query(date)
    print('querying...')
    run_query(date, query, database, s3_output) 

    # Get max count. 
    append_max()




# Pre-calculate. 
date = '20191108'
print('date:', date)
query_table(date)


















