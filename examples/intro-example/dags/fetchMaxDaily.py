import pandas as pd
import sys


output_filename = 'output/temp.csv'
df = pd.read_csv('input/output.csv')
audience = df[['audienceSegmentId', 'count']]
audience['count'] = audience['count'].fillna(0.0).astype(int)
header = ['audience_segment', 'max']


def maxToCsv():
    audience = audience.sort_values(by='audienceSegmentId')
    dict_as_max = {}
    for n,g in audience.groupby('audienceSegmentId'):
        max = g['count'].max()
        dict_as_max[int(n)] = max
    as_max_df = pd.DataFrame(list(dict_as_max.items()), columns = header)
    as_max_df.to_csv(output_filename, encoding='utf-8', index=False)


# def remove_outlier(df_in, col_name):
#     q1 = df_in[col_name].quantile(0.25)
#     q3 = df_in[col_name].quantile(0.75)
#     iqr = q3-q1 #Interquartile range
#     fence_low  = q1-1.5*iqr
#     fence_high = q3+1.5*iqr
#     df_out = df_in.loc[(df_in[col_name] > fence_low) & (df_in[col_name] < fence_high)]
#     return df_out





# for key, value in dict_audience.items():
