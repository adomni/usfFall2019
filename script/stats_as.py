import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('input/output.csv')
print(df.head())
audience = df[['audienceSegmentId', 'count']]


audience['count'] = audience['count'].fillna(0.0).astype(int)
print(audience.head())
audience = audience.sort_values(by='audienceSegmentId')
dict_audience = {}

for n,g in audience.groupby('audienceSegmentId'):
    dict_audience[n] = g

def remove_outlier(df_in, col_name):
    q1 = df_in[col_name].quantile(0.25)
    q3 = df_in[col_name].quantile(0.75)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_out = df_in.loc[(df_in[col_name] > fence_low) & (df_in[col_name] < fence_high)]
    return df_out

f = open("output/audience_segment/output.txt", "w+")
for key, value in dict_audience.items():
    print(key, file=f)
    print(value.describe(), file=f)
    print("-----------------------------------", file=f)
    removed_outlier = remove_outlier(value, 'count')
    plt.title("Histogram for Audience Segment: " + str(int(key)))
    plt.hist(removed_outlier['count'], bins='auto', normed=False)
    plt.ylabel('Frequency');
    plt.savefig("output/audience_segment/histogram/audience_segment_" + str(int(key)) + ".png", bbox_inches='tight',dpi=100)
    plt.close()


f.close()
