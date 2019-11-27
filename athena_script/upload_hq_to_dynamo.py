import csv
import boto3
from multiprocessing import Process
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
total_count = 0

dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table('high_quality')
threads = []
pool = ThreadPoolExecutor(max_workers=1000)


def convert_csv_to_json_list(file):
    count = 1
    items = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if 'as_one_id' in row and 'as_two_id' in row and 'as_three_id' in row and 'count' in row and row['as_one_id'] != '' and row['as_two_id'] != '' and row['as_three_id'] != '' and row['count'] != '':
                print(count)
                data = {}
                data['billboard_id'] = row['billboard_id']
                audience_segment_id = row['as_one_id'] + '_' + row['as_two_id'] + '_' + row['as_three_id']
                data['audience_segment_id'] = audience_segment_id
                data['count'] = row['count']
                #populate remaining fields here
                pool.submit(batch_write,data)
                # t3 = Thread(target=batch_write, args=(data,))
                # threads.append(t3)
                # t3.start()
                #batch_write(data)
            else:
                print('no')
            count = count + 1
    total_count = count
    #return items

def batch_write(item):
   with db.batch_writer() as batch:
     batch.put_item(Item=item)

if __name__ == '__main__':
   convert_csv_to_json_list('hq_20190308.csv')

pool.shutdown(wait=True)
