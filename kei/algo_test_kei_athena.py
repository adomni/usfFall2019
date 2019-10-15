import os
import sys
import csv
import boto3
import botocore
from retrying import retry
import pandas as pd

# configuration
# s3_bucket = 'adomni-placeiq-sync/neon_query_temp'  # S3 Bucket name
s3_bucket = 'aws-athena-query-results-734644148268-us-east-1'
s3_output  = 's3://'+ s3_bucket   # S3 Bucket to store results
database  = 'default'  # The database to which the query belongs

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')


@retry(
    # Try it until 3 minutes have passed.
    stop_max_delay = 1000 * 60 * 3,   
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


def run_query(query, database, s3_output):
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
        filename = query_execution_id + '.csv'
        
        # Download result file. 
        try:
            s3.Bucket(s3_bucket).download_file(s3_key, 'data/' + filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        
        # Read file to array. 
        rows = []
        with open('data/' + filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rows.append(row)

        # Delete result file.
#         if os.path.isfile(filename):
#             os.remove(filename)

        return rows

    if result['QueryExecution']['Status']['State'] == 'FAILED':
        print()
        print("Query FAILED: ")
        print(result['QueryExecution']['Status']['StateChangeReason'])


def make_query(billboard_id, audience_id):
    # audience = 'Dining->Coffee->Starbucks'
    # billboard_id = 'dbb561c792f78028f262e88ce95f857c'

    # audience_id = '257'
    # billboard_id = 'dbb561c792f78028f262e88ce95f857c'

    query = """
    SELECT
        a.billboard_id, 
        count(a.mobile_device_id) my_count
    FROM 
        location_data.billboard_devices_partitioned a
    INNER JOIN
        location_data.device_audiences_partitioned b
    ON 
        a.mobile_device_id = b.mobile_device_id
    INNER JOIN
        location_data.adomni_audience_segment c
    ON
        b.audience = c.placeiqid
    WHERE
        a.billboard_id = '%s'
        AND
        c.id = '%s'
    GROUP BY 
        a.billboard_id
    """ % (billboard_id, audience_id)

    return query



# def get_audience(audience_id):




def query_table(billboard_id, audience_id):

    query = make_query(billboard_id, audience_id)
    # This takes about 2 minutes. 
    response = run_query(query, database, s3_output)      

    return response


def get_count_map(billboard_id, audience_ids):
    # aud_seg_to_count (Key: audience_segment_id, Value: count)
    aud_seg_to_count = {}
    for audience_id in audience_ids:
        print('querying... audience_id: {0}'.format(audience_id))
        res = query_table(billboard_id, audience_id)

        count = res[0]['my_count']
        aud_seg_to_count[audience_id] = count

    return aud_seg_to_count


def get_max_count(audience_id):
    # Take as input the result_max.csv here.
    max_counts = pd.read_csv('data/result_max.csv')
    max_count = max_counts[max_counts['id'] == audience_id]['max']

    return max_count.values[0]


def get_normarized_count(aud_seg_to_count, audience_ids):
    for audience_id in audience_ids:
        count = aud_seg_to_count[audience_id]
        max_count = get_max_count(int(audience_id)) 

        normalized = int(count) / max_count
        aud_seg_to_count[audience_id] = normalized

    return aud_seg_to_count


#precalculate values at night time for maximum?
#using dynamo or athena data for each segmentId
#append the result into a database 

#calculate score
def calculateScore(billboard_id, audience_ids):
    score = 0


    #access the precalculated values in the above database to compare the count of locationHash with the precalculated statistic for that audienceId

    # aud_seg_to_count (Key: audience_id, Value: count)
    aud_seg_to_count = get_count_map(billboard_id, audience_ids)

    # aud_seg_to_normalized (Key: audience_segment_id, Value: normalized count)
    aud_seg_to_normalized = get_normarized_count(aud_seg_to_count, audience_ids)

    print(aud_seg_to_normalized)

    #score and save the array of all the result for each audienceId and calulate the average



        

    #return the result 
    return score



billboard_id = 'dbb561c792f78028f262e88ce95f857c'
# audience = 'Dining->Coffee->Starbucks'
audience_ids = ['257', '30']
calculateScore(billboard_id, audience_ids)























