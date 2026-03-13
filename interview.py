import pandas as pd
import warnings
warnings.filterwarnings('ignore')

source='./5min_candles/46900PE.csv'

data=pd.read_csv(source)

# Task 1:
#printing the OHLC values at timestamp=2024-01-10 09:25:00

timestamp='2024-01-10 09:25:00'
print(data[data['date']==timestamp])

#Task 2:
#printing which has higher 'high' value
timestamp1='2024-01-10 09:20:00'
timestamp2='2024-01-10 09:25:00'

high1=int(data[data['date']==timestamp1]['high'])
high2=int(data[data['date']==timestamp2]['high'])

if int(high1)>high2: print('yes trend continues')
else: print('trend does not hold true')