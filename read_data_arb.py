import pandas as pd
import sqlalchemy
from binance.client import Client
import talib
import time
import warnings
warnings.filterwarnings('ignore')

def initial_data(symbol, date):
    new_data = getdata(symbol=symbol, start=date)
    new_rows = new_data[new_data.index > date]
    new_rows[:-1].to_sql(symbol, engine, if_exists='append')
    print(str(len(new_rows[:-1])) + 'new rows imported to DB' + '' + symbol)

def getdata(symbol, start):
    frame = pd.DataFrame(client.futures_historical_klines(symbol, '1m', start))
    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Opne', 'High', 'Low', 'Close', 'Volume']
    frame.set_index('Time', inplace=True)
    frame.index = pd.to_datetime(frame.index, unit='ms')
    output = pd.DataFrame(talib.EMA(frame['Close'], timeperiod=30))
    merged_df = pd.merge(frame, output, left_index=True, right_index=True, how='outer')
    merged_df = merged_df.astype(float)
    return merged_df

def crypto_kline_SQL_updater(symbol,start):
    max_date = pd.read_sql(f'select max(Time) from {symbol}', engine).values[0][0]
    print(max_date)
    new_data = getdata(symbol, start)
    new_rows = new_data[new_data.index > max_date]
    new_rows[:-1].to_sql(symbol, engine, if_exists='append')
    print(str(len(new_rows[:-1])) + 'new rows imported to DB' + '' + symbol)

client = Client()
engine = sqlalchemy.create_engine('sqlite:///crypto0316_db.db')

begain_date = '2023-3-27'


initial_data('ARBUSDT', begain_date)

while True:
    try:
        crypto_kline_SQL_updater('ARBUSDT', begain_date)
    except:
        print('erro')

    time.sleep(30)