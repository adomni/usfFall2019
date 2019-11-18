"""
    Simple lambda trigger doing something when a file is uploaded to s3
"""

import json
import boto3
from algo_test import calculate_score
# Set all these variables when you upload (
bucket_name = 'lambda-output-test2'
locationHash = '50158cf1c6fded24e3b510d0d6dbd8e3'
algorithm = 'KEI'
unconverted_prefix='unconverted'
converted_prefix='ready'

audience_ids = ['44', '61', '748']

# While a file shows up in the unconverted bucket, run our processor
def start_usf_processor(event, context):

    #print ("Processing usf handler for " + json.dumps(event))

    try:
        if (event!=None and 'Records' in event and
                len(event.get('Records'))==1 and
                's3' in event.get('Records')[0] and
                'object' in event.get('Records')[0].get('s3') and
                'key' in event.get('Records')[0].get('s3').get('object')):

            s3_object = event.get('Records')[0].get('s3').get('object')
            infile_key = s3_object.get('key')

            #return {'status' : 'ok', 'message' : str(score)}

            if (infile_key.startswith(unconverted_prefix)):
                input_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
                input_json = get_s3(input_bucket, infile_key)

                outfile_key = converted_prefix+('.'.join(infile_key[len(unconverted_prefix):].split('.')[:-1]) + '.json')
                print("Started ok, outfile is " + outfile_key)
                score = calculate_score(locationHash, audience_ids)
                data = {
                    "request": {
                        "locationHash":locationHash,
                        "algorithm": algorithm,
                        "audienceSegmentIds": audience_ids
                    },
                    "score" : score,
                    "errors": []
                }
                with open('temp/' + outfile_key, 'w') as outfile:
                    json.dump(data, outfile)
                put_s3(outfile_key)
                return {'status' : 'ok', 'message' : outfile_key}
            else :
                return {'status' : 'ignored', 'message' : 'wrong path'}

        else :
            return {'status' : 'ignored', 'message':'Invalid input'}

    except Exception as exception:
        print("Failed to execute : " + str(exception))
        return {'status' : 'error',
                'message' : str(exception)}

def get_s3(bucket, filename):
    s3_res = boto3.resource('s3')
    content_object = s3_res.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    print(json_content)
    return json_content


def put_s3(filename):
    s3_client = boto3.client('s3')

    with open('temp/' + filename) as file:
        object = file.read()
        s3_client.put_object(Body=object, Bucket=bucket_name, Key=filename, ContentType='json')
        print("successfully uploaded")


# For running tests
with open ("test-s3-put.json", "r") as myfile:
    event = json.loads(myfile.read())

context = {}
print("---------------------------------------")
print(str(start_usf_processor(event, context)))
print("---------------------------------------")
