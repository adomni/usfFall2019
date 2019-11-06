"""
    Simple lambda trigger doing something when a file is uploaded to s3
"""

import json

# Set all these variables when you upload (
bucket_name = 'your-bucket'
unconverted_prefix='unconverted'
converted_prefix='ready'

# While a file shows up in the unconverted bucket, run our processor
def start_usf_processor(event, context):

    print ("Processing usf handler for " + json.dumps(event))

    try:
        if (event!=None and event.has_key('Records') and
                len(event.get('Records'))==1 and
                event.get('Records')[0].has_key('s3') and
                event.get('Records')[0].get('s3').has_key('object') and
                event.get('Records')[0].get('s3').get('object').has_key('key')):

            s3_object = event.get('Records')[0].get('s3').get('object')
            infile_key = s3_object.get('key')

            if (infile_key.startswith(unconverted_prefix)):
                outfile_key = converted_prefix+('.'.join(infile_key[len(unconverted_prefix):].split('.')[:-1]) + '.json')
                print("Started ok, outfile is " + outfile_key)
                return {'status' : 'ok'}
            else :
                return {'status' : 'ignored', 'message' : 'wrong path'}

        else :
            return {'status' : 'ignored', 'message':'Invalid input'}

    except Exception as exception:
        print("Failed to execute : " + str(exception))
        return {'status' : 'error',
                'message' : str(exception)}


# For running tests
with open ("test-s3-put.json", "r") as myfile:
    event = json.loads(myfile.readlines())

context = {}
start_usf_processor(event, context)
