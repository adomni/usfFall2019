import json
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr
import boto3
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import torch
import torch.nn as nn
from torch.autograd import Variable
from sklearn.preprocessing import MinMaxScaler

with open('DynamoDBtoCSV-master/config.json') as f:
    Tuo_authentications = json.load(f)


def run_query(query, s3_output):
    client = boto3.client('athena',
                           aws_access_key_id = Tuo_authentications['accessKeyId'],
                           aws_secret_access_key = Tuo_authentications['secretAccessKey'],
                           region_name = Tuo_authentications["region"])
    response = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
          'Database': 'default'
        },
        ResultConfiguration = {
            'OutputLocation': s3_output,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
        
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response


s3_output = 's3://tuo-bucket/playing'

query = "select * from adomni_audience_segment where id='{ID}';"

params = {"ID":"740"}

QueryResponse = run_query(query.format(**params), s3_output)
QueryExecutionId = QueryResponse['QueryExecutionId']


with open('DynamoDBtoCSV-master/config.json') as f:
    Tuo_authentications = json.load(f)

dynamodb = boto3.resource('dynamodb',
                           aws_access_key_id = Tuo_authentications['accessKeyId'],
                           aws_secret_access_key = Tuo_authentications['secretAccessKey'],
                           region_name = Tuo_authentications["region"]
)


table = dynamodb.Table('machine_learning')

response = table.query(
    KeyConditionExpression=Key('billboard_audience_segment_id').eq('b01186c0cdbde739e62d794f13792c36623')
)
data = response['Items']

df = pd.DataFrame(data)

training_set = df.iloc[:,1].values
training_set = [[float(i)] for i in training_set]

def sliding_windows(data, seq_length):
    x = []
    y = []

    for i in range(len(data)-seq_length-1):
        _x = data[i:(i+seq_length)]
        _y = data[i+seq_length]
        x.append(_x)
        y.append(_y)

    return np.array(x),np.array(y)

sc = MinMaxScaler()
training_data = sc.fit_transform(training_set)

seq_length = 20
x, y = sliding_windows(training_data, seq_length)

train_size = int(len(y) * 0.67)
test_size = len(y) - train_size

dataX = Variable(torch.Tensor(np.array(x)))
dataY = Variable(torch.Tensor(np.array(y)))

trainX = Variable(torch.Tensor(np.array(x[0:train_size])))
trainY = Variable(torch.Tensor(np.array(y[0:train_size])))

testX = Variable(torch.Tensor(np.array(x[train_size:len(x)])))
testY = Variable(torch.Tensor(np.array(y[train_size:len(y)])))

class LSTM(nn.Module):

    def __init__(self, num_classes, input_size, hidden_size, num_layers):
        super(LSTM, self).__init__()
        
        self.num_classes = num_classes
        self.num_layers = num_layers
        self.input_size = input_size
        self.hidden_size = hidden_size
        
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=hidden_size,
                            num_layers=num_layers, batch_first=True)
        
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        h_0 = Variable(torch.zeros(
            self.num_layers, x.size(0), self.hidden_size))
        
        c_0 = Variable(torch.zeros(
            self.num_layers, x.size(0), self.hidden_size))
        
        # Propagate input through LSTM
        ula, (h_out, _) = self.lstm(x, (h_0, c_0))
        
        h_out = h_out.view(-1, self.hidden_size)
        
        out = self.fc(h_out)
        
        return out

num_epochs = 2000
learning_rate = 0.01

input_size = 1
hidden_size = 2
num_layers = 1

num_classes = 1

lstm = LSTM(num_classes, input_size, hidden_size, num_layers)

criterion = torch.nn.MSELoss()    # mean-squared error for regression
optimizer = torch.optim.Adam(lstm.parameters(), lr=learning_rate)
#optimizer = torch.optim.SGD(lstm.parameters(), lr=learning_rate)

# Train the model
for epoch in range(num_epochs):
    outputs = lstm(trainX)
    optimizer.zero_grad()
    
    # obtain the loss function
    loss = criterion(outputs, trainY)
    
    loss.backward()
    
    optimizer.step()
    if epoch % 100 == 0:
        print("Epoch: %d, loss: %1.5f" % (epoch, loss.item()))

lstm.eval()
train_predict = lstm(dataX)

data_predict = train_predict.data.numpy()
dataY_plot = dataY.data.numpy()

data_predict = sc.inverse_transform(data_predict)
dataY_plot = sc.inverse_transform(dataY_plot)

plt.figure(figsize=(20,8))

print(len(dataX))
plt.plot(dataY_plot, "g", label="real counts", linewidth=2.0)
plt.plot(data_predict, "y", label="predicted counts of LSTM", linewidth=2.0)


plt.xticks(range(len(dataY_plot)))
plt.xlabel('Prediction points')
plt.ylabel('Counts')
plt.legend(loc="best")
# plt.suptitle('Time-Series Prediction')
plt.savefig("Time-Series Prediction.png")
plt.show()

from sklearn.metrics import mean_squared_error
mse = mean_squared_error(dataY_plot, data_predict)
print("RMSE:", np.sqrt(mse))