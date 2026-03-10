import pandas as pd
import os

source_folder='./data'
output_folder='./5min_candles'
fib_df=pd.DataFrame(columns=['file','Pivot','R1','R2','R3','S1','S2','S3'])

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
        
        #storing 1 Day interval OHLC
        df_1D= df.resample('D').agg({
            'open':'first',
            'high':'max',
            'low':'min',
            'close':'last'
        }).dropna()
        open=df_1D.loc['2024-01-10','open']
        high=df_1D.loc['2024-01-10','high']
        low=df_1D.loc['2024-01-10','low']
        close=df_1D.loc['2024-01-10','close']

        # Calculating Fibonacci Pivot Points
        Pivot=(high+low+close)/3
        R1=Pivot + 0.382*(high-low)
        R2=Pivot + 0.618*(high-low)
        R3=Pivot + (high-low)
        S1=Pivot - 0.382*(high-low)
        S2=Pivot - 0.618*(high-low)
        S3=Pivot - (high-low)
        fib=[file,Pivot,R1,R2,R3,S1,S2,S3]

        fib_df.loc[len(fib_df)]=fib
    

print("fibonacci levels for 2024-01-10 are:")
print(fib_df)

