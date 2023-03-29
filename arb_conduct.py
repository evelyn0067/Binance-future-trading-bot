import pandas as pd
import sqlalchemy
from binance.client import Client
import time
import warnings
warnings.filterwarnings('ignore')

def get_initial_amount(symbol,start):
    price = float(client.futures_historical_klines(symbol, '1d', start)[0][1])
    return price

def price_SQL_updater():
    colums_name = ['Time']
    price_list = [time.time()]
    for x in coinlist:
        price_list.append(float((client.futures_symbol_ticker(symbol=x)['price'])))
        colums_name.append(x)
    frame = pd.DataFrame(columns=colums_name)
    frame.loc[len(frame)] = price_list
    frame.to_sql('pricetable',engine,if_exists='append')

def order_SQL_updater(order):
    df = pd.DataFrame(order,index=[0])
    df.to_sql('order',engine,if_exists='append')

def check_position_information(symbol, miniamount):
    account = client.futures_account()
    positionlist = []
    existshortposition = False
    existlongposition = False
    int(miniamount)
    longpositionamount = 0
    shortpositionamount = 0
    long_position_roe = 0
    short_position_roe = 0
    for b1 in account['positions']:
        if b1['symbol'] == symbol and float(b1['initialMargin']) > 0:
            d = dict()
            d['initialmargin'] = float(b1['initialMargin'])
            d['unrealizedprofit'] = float(b1['unrealizedProfit'])
            d['amt'] = float(b1['positionAmt'])
            d['roe'] = round(float(b1['unrealizedProfit']) / float(b1['initialMargin']), 3)
            d['side'] = b1['positionSide']
            side = b1['positionSide']
            if side == 'LONG':
                longpositionamount = float(b1['positionAmt'])
                long_position_roe = d['roe']
                if longpositionamount >= miniamount:
                    existlongposition = True
                else:
                    existlongposition = False
            elif side == 'SHORT':
                shortpositionamount = abs(float(b1['positionAmt']))
                short_position_roe = d['roe']
                if shortpositionamount >= miniamount:
                    existshortposition = True
                else:
                    existshortposition = False
            positionlist.append(d)

    positionlist1 = {'positioninfo': positionlist,
                     'existshortposition': existshortposition,
                     'existlongposition': existlongposition,
                     'exist_shortposition_amount': shortpositionamount,
                     'exist_longposition_amount': longpositionamount,
                     'long_position_roe': long_position_roe,
                     'short_position_roe': short_position_roe}
    return positionlist1

def simple_strategy(symbol, lookback):
    df = pd.read_sql(symbol, engine)
    EMA144 = round(df['0'].iloc[-1], 4)
    df_price = pd.read_sql('arbpricetable', engine)
    price1 = df_price[symbol].iloc[-1]
    position = check_position_information(symbol = symbol, miniamount=2/3*simple_strategy.qty)
    print(simple_strategy.qty,position)
    price2 = float((client.futures_symbol_ticker(symbol=symbol)['price']))
    price_list.append(price2)
    colums_name = ['time', 'ARBUSDT']
    frame = pd.DataFrame(columns=colums_name)
    frame.loc[len(frame)] = price_list
    frame.to_sql('arbpricetable', engine, if_exists='append')
    print(price1, price2, EMA144)
    if price2 > EMA144 and EMA144 >= price1 and round(float(position['exist_longposition_amount']),0)==0:
        if round(float(position['exist_shortposition_amount']), 0) > 0:
            simple_strategy.counter += 1
            order = client.futures_create_order(symbol=symbol,
                                        type ='MARKET',
                                        side ='BUY',
                                        quantity =round(float(position['exist_shortposition_amount']), 0),
                                        positionSide ='SHORT')
            print(order)
            order1 = client.futures_create_order(symbol=symbol,
                                                type='MARKET',
                                                side='BUY',
                                                quantity=round(simple_strategy.qty, 0),
                                                positionSide='LONG')
            print(order1)
            order_SQL_updater(order1)
            simple_strategy.qty = 2 * simple_strategy.qty
        else:
            simple_strategy.counter += 1
            order1 = client.futures_create_order(symbol=symbol,
                                                type='MARKET',
                                                side='BUY',
                                                quantity=round(simple_strategy.qty,0),
                                                positionSide='LONG')
            print(order1)
            simple_strategy.qty = 2 * simple_strategy.qty
    if price1 >= EMA144 and price2 < EMA144 and round(float(position['exist_shortposition_amount']),0)==0:
        if round(float(position['exist_longposition_amount']), 0) > 0:
            simple_strategy.counter += 1
            order = client.futures_create_order(symbol=symbol,
                                        type = 'MARKET',
                                        side = 'SELL',
                                        quantity = round(float(position['exist_longposition_amount']), 0),
                                        positionSide='LONG')
            print(order)
            order1 = client.futures_create_order(symbol=symbol,
                                                type='MARKET',
                                                side='SELL',
                                                quantity= round(simple_strategy.qty,0),
                                                positionSide='SHORT')
            simple_strategy.qty = 2 * simple_strategy.qty
        else:
            simple_strategy.counter += 1
            order1 = client.futures_create_order(symbol=symbol,
                                                type='MARKET',
                                                side='SELL',
                                                quantity= round(simple_strategy.qty,0),
                                                positionSide='SHORT')
            print(simple_strategy.qty)
            print(order1)
            order_SQL_updater(order1)
            simple_strategy.qty = 2 * simple_strategy.qty

    else:
        if position['long_position_roe'] < -0.08:
           client.futures_create_order(symbol=symbol,
                                       type='MARKET',
                                       side='SELL',
                                       quantity=round(float(position['exist_longposition_amount']),0),
                                       positionSide='LONG')
        elif position['long_position_roe'] > 0.3 and simple_strategy.qty == 0:
            client.futures_create_order(symbol=symbol,
                                        type='MARKET',
                                        side='SELL',
                                        quantity=round(simple_strategy.qty*0.3, 0),
                                        positionSide='LONG')
        elif position['short_position_roe'] < -0.08:
            client.futures_create_order(symbol=symbol,
                                        type='MARKET',
                                        side='BUY',
                                        quantity=round(float(position['exist_shortposition_amount']), 0),
                                        positionSide='SHORT')
        elif position['short_position_roe'] > 0.3 and simple_strategy.qty == 0:
            client.futures_create_order(symbol=symbol,
                                        type='MARKET',
                                        side='BUY',
                                        quantity=round(simple_strategy.qty*0.3, 0),
                                        positionSide='SHORT')

        if position['long_position_roe']>0.1 or position['long_position_roe']>0.1:
            simple_strategy.qty = 6


## initial information
client = Client('Public_Key', 'Private_key', {"verify": False, "timeout":5})
engine = sqlalchemy.create_engine('sqlite:///crypto0316_db.db')
coinlist = ['ARBUSDT']
amountlist = dict()
start = '2023-03-26'
usdt_amount = 0.5
leverage = 12
simple_strategy.counter = 0
simple_strategy.qty = 6

for x in coinlist:
    client.futures_change_leverage(symbol=x, leverage=leverage)
    price = get_initial_amount(symbol=x, start=start)
    qty = round((leverage*usdt_amount/len(coinlist)/price),0)
    amountlist[x] = qty
print(amountlist)

while True:
    try:
        price_list=[time.time()]
        simple_strategy(symbol='ARBUSDT', lookback=144)
    except:
        print('error')

