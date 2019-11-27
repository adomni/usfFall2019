"""
    Simple lambda trigger doing something when a file is uploaded to s3
"""

import json
import boto3
import pytz
from datetime import datetime
from algo_test2 import calculate_score
# Set all these variables when you upload (

unconverted_prefix='input'
converted_prefix='output'

# While a file shows up in the unconverted bucket, run our processor
def start_usf_processor(event, context):

    tz = pytz.timezone('America/Los_Angeles')
    timestamp_st = str(datetime.now(tz))
    print (timestamp_st + " : Processing usf handler for " + json.dumps(event))

    try:
        if (event!=None and 'Records' in event and
                len(event.get('Records'))==1 and
                's3' in event.get('Records')[0] and
                'object' in event.get('Records')[0].get('s3') and
                'key' in event.get('Records')[0].get('s3').get('object')):

            s3_object = event.get('Records')[0].get('s3').get('object')
            infile_key = s3_object.get('key')

            # return {'status' : 'ok', 'message' : str(score)}

            if (infile_key.startswith(unconverted_prefix)):
                outfile_key = converted_prefix+('.'.join(infile_key[len(unconverted_prefix):].split('.')[:-1]) + '.json')
                input_json = {"message":"Not read yet"}
                input_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
                input_json = get_s3(input_bucket, infile_key)
                if 'locationHash' in input_json and 'algorithm' in input_json and 'audienceSegmentIds' in input_json:
                    locationHash = input_json['locationHash']
                    algorithm = input_json['algorithm']
                    audience_ids = input_json['audienceSegmentIds']
                    print("Started ok, outfile is " + outfile_key)
                    score = calculate_score(locationHash, audience_ids, algorithm)
                    # score = 42 # The best number
                    data = {
                        "request": input_json,
                        "generated": timestamp_st,
                        "score" : score,
                        "errors": []
                    }
                    put_s3(input_bucket, outfile_key, data)
                    return {'status' : 'ok', 'message' : outfile_key}
                else:
                    return {'status' : 'ignored', 'message' : 'invalid input json format'}
            else :
                return {'status' : 'ignored', 'message' : 'wrong path'}

        else :
            return {'status' : 'ignored', 'message':'Invalid input'}

    except Exception as exception:
        print("Failed to execute : " + str(exception))
        try:
            fail_data = {
                "request": input_json,
                "generated": timestamp_st,
                "score": -1,
                "errors": [str(exception)]
            }
            put_s3(input_bucket, outfile_key, fail_data)
        except Exception as err2:
            print("Bad - failed to store the failure value : " + str(err2))
        return {'status' : 'error',
                'message' : str(exception)}

def get_s3(bucket, filename):
    print('Loading ' + bucket + ' : '+filename)
    s3_res = boto3.resource('s3')
    content_object = s3_res.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    print(json_content)
    return json_content


def put_s3(bucket_name, filename, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=filename, ContentType='application/json')
    print("successfully uploaded to " + bucket_name + " : " + filename)


# For running tests
with open ("test-s3-put.json", "r") as myfile:
    event = json.loads(myfile.read())

context = {}
print("---------------------------------------")
print(str(start_usf_processor(event, context)))
print("---------------------------------------")
