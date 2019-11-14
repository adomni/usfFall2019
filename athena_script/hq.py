import boto3
from credentials import access_key, secret_key, region_name
from time import sleep
import os
from multiprocessing import Process
from threading import Thread
import pandas as pd
from datetime import datetime
import time
import sys

#things to do prepare ml dataset and higher quality mobile devices
#also star tworking on aws lambda
#athena client init
#future
from boto3.session import Session
from boto3.dynamodb.conditions import Key
pd.set_option('display.max_columns', 7)
dynamodb_session = Session(aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)

dynamodb = dynamodb_session.resource('dynamodb')
table = dynamodb.Table('high_quality')

table.put_item(Item = {'billboard_id': 'test', 'audience_segment_id': 'test', 'test' :'test'})

overwrite = False

print('------------------------------------------')
print('Creating max count for each segment id dataset...')
print('------------------------------------------')

if len(sys.argv) == 2:
    if sys.argv[1] == 'overwrite':
        print('Overwrite Mode On')
        overwrite = True
else:
    print('Overwrite Mode Off')

client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)

threads = []
output_filename_one = 'result_max.csv'
output_filename_two = 'result_ml.csv'
output_filename_three = 'result_hq.csv'

available_dates = ['20190308',]

header_one = ['audience_segment_id', 'max']
header_two = ['billboard_id', 'audience_segment_id', 'count', 'year', 'quarter', 'month', 'week_of_year']
header_three = ['billboard_id', 'audience_id_one', 'audience_id_two', 'audience_id_three', 'count']
alphabet = ['a', 'b', 'c', 'd', 'e', 'f']

# age_segment = [39, 40, 41, 42, 43, 44, 45, 46, 47]
# gender_segment [60, 61]
# all_other_segment = [737,738,739,741,740,743,742,744,746,745,748,747,749,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,56,57,58,59,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,735,91,92,93,94,95,96,97,750,733,98,99,734,100,101,102,103,104,105,106,796,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,761,762,767,779,768,763,769,770,780,771,764,772,773,786,774,775,776,777,791,781,778,787,788,789,783,784,782,785,790,765,766,792,793,794,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,795,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,751,753,757,754,752,760,755,756,758,759,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,798,799,800,801,802,803,804,805,806,808,809,810,811,812,813,814,815,816,817,818,819,820,821,797,807,822,823,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,736,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,395,394,397,396,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,825,826,827,828,824,829,830,831,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732]

#get Max count for each audience_segment_id
# def runMaxCount(num, date):
#
#     query = """
#     SELECT id as audience_segment_id, max(count) as max FROM (SELECT a.billboard_id, c.id, count(distinct a.mobile_device_id) as count FROM
#     (SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=""" + date + """ and billboard_id like '""" + num + """%') a LEFT JOIN
#     (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=""" + date + """) b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
#     location_data.adomni_audience_segment c ON b.audience = c.placeiqid
#     GROUP BY a.billboard_id, c.id) abc GROUP BY id ORDER BY id asc
#     """
#
#     response = client.start_query_execution(
#         QueryString = query,
#         QueryExecutionContext = {'Database': 'default'},
#         ResultConfiguration = {
#             'OutputLocation': 's3://athena-output-usf',
#             'EncryptionConfiguration': {
#             'EncryptionOption': 'SSE_S3'
#             }
#         }
#     )
#     id = response['QueryExecutionId']
#
#     while (client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] == 'RUNNING'):
#         #print("Waiting... "  + num)
#         sleep(5)
#     status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
#     reason = ""
#     if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
#         reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
#     status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
#     outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']
#     output_path = 'max_' + date + '_' + num + '.csv'
#     if status is not "FAILED":
#         os.system("aws s3 cp " + outputLocation + " .")
#         os.system("rm " + output_path)
#         os.system("mv " + id + ".csv " + output_path)
#     #print(status_query)
#     print("Max for " + num + ' ' + date)
#     print(status)
#     print(reason)
#     #print(outputLocation)
#
# #returns and saves count for each billboard id and audience segment
# def runGetCount(num, date):
#
#     query = """
#     SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
#     (SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=""" + date + """ and billboard_id like '""" + num + """%') a LEFT JOIN
#     (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=""" + date + """) b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
#     location_data.adomni_audience_segment c ON b.audience = c.placeiqid
#     GROUP BY a.billboard_id, c.id
#     """
#
#     response = client.start_query_execution(
#         QueryString = query,
#         QueryExecutionContext = {'Database': 'default'},
#         ResultConfiguration = {
#             'OutputLocation': 's3://athena-output-usf',
#             'EncryptionConfiguration': {
#             'EncryptionOption': 'SSE_S3'
#             }
#         }
#     )
#     id = response['QueryExecutionId']
#
#     while (client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] == 'RUNNING'):
#         #print("Waiting... "  + num)
#         sleep(5)
#     status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
#     reason = ""
#     if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
#         reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
#     status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
#     outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']
#     output_path = 'ml_' + date + '_' + num + '.csv'
#     if status is not "FAILED":
#         os.system("aws s3 cp " + outputLocation + " .")
#         os.system("rm " + output_path)
#         os.system("mv " + id + ".csv " + output_path)
#     #print(status_query)
#     print('ML for ' + num + ' ' + date)
#     print(status)
#     print(reason)
#     #print(outputLocation)

def runHQCount(date):
    output_path = 'hq_' + date + '.csv'
    if os.path.exists(output_path) and not overwrite:
        print(output_path + ' already saved')
    else:
        print("Querying... for " + output_path)
        query = """
        SELECT g.billboard_id, f.as_one_id, f.as_two_id, f.as_three_id, COUNT(*) as count FROM (SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=""" + date + """) g LEFT JOIN
        (SELECT c.mobile_device_id, c.id as as_one_id, d.id as as_two_id, e.id as as_three_id FROM
        (SELECT a.mobile_device_id, id FROM (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=""" + date + """) a left join location_data.adomni_audience_segment b on a.audience = b.placeiqid where id in ('39', '40', '41', '42', '43', '44', '45', '46', '47')) c
        INNER JOIN
        (SELECT a.mobile_device_id, id FROM (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=""" + date + """) a left join location_data.adomni_audience_segment b on a.audience = b.placeiqid where id in ('60', '61')) d
        ON c.mobile_device_id = d.mobile_device_id
        INNER JOIN
        (SELECT a.mobile_device_id, id FROM (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=""" + date + """) a left join location_data.adomni_audience_segment b on a.audience = b.placeiqid where id not in ('39', '40', '41', '42', '43', '44', '45', '46', '47', '60', '61')) e
        ON c.mobile_device_id = e.mobile_device_id) f
        ON g.mobile_device_id = f.mobile_device_id GROUP BY 1, 2, 3, 4
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
            #print("Waiting... "  + num)
            sleep(5)
        status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
        reason = ""
        if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
            reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
        status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
        outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']

        if status is not "FAILED":
            os.system("aws s3 cp " + outputLocation + " .")
            os.system("rm " + output_path)
            os.system("mv " + id + ".csv " + output_path)
        #print(status_query)
        print("HQ for " + date)
        print(status)
        print(reason)
        #print(outputLocation)

start_time = time.time()

for d in available_dates:
    # for i in range (0, 10):
    #     t1 = Thread(target=runGetCount, args=(str(i), d))
    #     threads.append(t1)
    #     t1.start()
    #     t2 = Thread(target=runMaxCount, args=(str(i), d))
    #     threads.append(t2)
    #     t2.start()
    #     sleep(5)
    #
    # for a in alphabet:
    #     t1 = Thread(target=runGetCount, args=(str(a), d))
    #     threads.append(t1)
    #     t1.start()
    #     t2 = Thread(target=runMaxCount, args=(str(a), d))
    #     threads.append(t2)
    #     t2.start()
    #     sleep(5)
    t3 = Thread(target=runHQCount, args=(d,))
    threads.append(t3)
    t3.start()
    t3.join()
    threads.remove(t3)

for x in threads:
    x.join()

combined_max_df = pd.DataFrame(columns=header_one)
result_max_df = pd.DataFrame(columns=header_one)
combined_ml_df = pd.DataFrame(columns=header_two)

#for max
# for d in available_dates:
#     for i in range (0, 10):
#         temp_df = pd.read_csv("max_" + d + "_" + str(i) + ".csv")
#         combined_max_df = combined_max_df.merge(temp_df, how='outer')
#         os.system("rm max_" + d + "_" + str(i) + ".csv")
#
#     for a in alphabet:
#         temp_df = pd.read_csv("max_" + d + "_" + str(a) + ".csv")
#         combined_max_df = combined_max_df.merge(temp_df, how='outer')
#         os.system("rm max_" + d + "_" + str(a) + ".csv")
#
#
# for n,g in combined_max_df.groupby('audience_segment_id'):
#      max = g['max'].max()
#      result_max_df = result_max_df.append({'id': n, 'max': max}, ignore_index= True)
#
# #for ml
# for d in available_dates:
#     for i in range (0, 10):
#         temp_df = pd.read_csv("result_" + str(i) + "_" + d + ".csv")
#         date_obj = datetime.strptime(d, '%Y%m%d')
#         temp_df['date'] = date_obj.strftime('%Y-%m-%d')
#         temp_df.date = pd.to_datetime(temp_df.date)
#         temp_df['year'] = date_obj.strftime('%Y')
#         temp_df['quarter'] = pd.PeriodIndex(temp_df.date, freq='Q')
#         temp_df['month'] = date_obj.strftime('%m')
#         temp_df['week_of_year'] = date_obj.strftime("%V")
#         temp_df = pd.merge(temp_df, result_max_df, how='left', on=['audience_segment_id'])
#         temp_df.count = temp_df.count / temp_df.max
#         temp_df = temp_df.drop('max', 1)
#         temp_df['range'] = '0'
#         temp_df['range'][temp_df['count'] >= 0.2] = '1'
#         temp_df['range'][temp_df['count'] >= 0.4] = '2'
#         temp_df['range'][temp_df['count'] >= 0.6] = '3'
#         temp_df['range'][temp_df['count'] >= 0.8] = '4'
#         combined_ml_df = combined_ml_df.merge(temp_df, how='outer')
#         os.system("rm ml_" + d + "_" + str(a) + ".csv")
#
#     for a in alphabet:
#         temp_df = pd.read_csv("result_" + str(a) + "_" + d + ".csv")
#         date_obj = datetime.strptime(d, '%Y%m%d')
#         temp_df['date'] = date_obj.strftime('%Y-%m-%d')
#         temp_df.date = pd.to_datetime(temp_df.date)
#         temp_df['year'] = date_obj.strftime('%Y')
#         temp_df['quarter'] = pd.PeriodIndex(temp_df.date, freq='Q')
#         temp_df['month'] = date_obj.strftime('%m')
#         temp_df['week_of_year'] = date_obj.strftime("%V")
#         temp_df = pd.merge(temp_df, result_max_df, how='left', on=['audience_segment_id'])
#         temp_df.count = temp_df.count / temp_df.max
#         temp_df = temp_df.drop('max', 1)
#         temp_df['range'] = '0'
#         temp_df['range'][temp_df['count'] >= 0.2] = '1'
#         temp_df['range'][temp_df['count'] >= 0.4] = '2'
#         temp_df['range'][temp_df['count'] >= 0.6] = '3'
#         temp_df['range'][temp_df['count'] >= 0.8] = '4'
#         combined_ml_df = combined_ml_df.merge(temp_df, how='outer')
#         os.system("rm ml_" + d + "_" + str(a) + ".csv")
#
#
# for n,g in combined_ml_df.groupby('audience_segment_id'):
#     for n2, g2, in g.groupby('range'):
#         temp_filename = 'ml_' + n + '_' + n2 + ".csv"
#         g2.to_csv(temp_filename, encoding='utf-8', index=False)
#         print("Saved " + temp_filename)
#         os.system("aws s3 cp " + temp_filename + " s3://result-output/machine_learning/")

#for hq
for d in available_dates:
    temp_filename = 'hq_' + d + '.csv'
    final_df = pd.read_csv(temp_filename)
    print('to dict')
    dictdata = final_df.T.to_dict().values()
    for tempdata in dictdata:
        print('inserting')
        table.put_item(Item=tempdata)
    os.system("aws s3 cp " + temp_filename + " s3://result-output/high_quality/")

elapsed_time = time.time() - start_time
print('Finished. Elapsed Time: ' + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
#
# #print(result_df.head())
# result_max_df.to_csv(output_filename, encoding='utf-8', index=False)
# os.system("aws s3 cp " + output_filename + " s3://result-output/")
