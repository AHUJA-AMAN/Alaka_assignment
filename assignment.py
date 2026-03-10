import pandas as pd
import os

source_folder='./data'
output_folder='./5min_candles'

source_folder='./data'
output_folder='./5min_candles'


#incase output folder is not created
if not os.path.exists(output_folder):
   os.makedirs(output_folder)

for file in os.listdir(source_folder):
    if file.endswith('.parquet'):
        file_path=os.path.join(source_folder,file)
        df=pd.read_parquet(file_path)
        df['date']=pd.to_datetime(df['date'])

        #dropping dates other than 2024-01-10
        df=df[df['date'].dt.strftime('%Y-%m-%d')=='2024-01-10']
        df.set_index('date', inplace=True)
        


        #storing 5 min intervals
        df_5min= df.resample('5min').agg({
            'open':'first',
            'high':'max',
            'low':'min',
            'close':'last'
        }).dropna()

        csv_filename=file.replace('.parquet','.csv')
        df_5min.to_csv(os.path.join(output_folder,csv_filename))
        print(f"Converted {file}")



