import pandas as pd
import requests

import datetime as dt
import pytz     as pytz
import time     as time

from tqdm import tqdm

from aiohttp.client import request
import asyncio
import aiohttp
import json


from ...__utility__ import config 
TQDM_OFF :  bool = True
FAIL_STACK       = []

def cooldown():
    timezone     : pytz.timezone = pytz.timezone('America/New_York')
    wait_seconds : int           = 60 - dt.datetime.now().second
    
    time.sleep(wait_seconds)
    print('[+1] ', dt.datetime.now(timezone))

async def fetch_bars(ticker, payload, session):
    try:
        endpoint = APCA_API_BASE_URL + '/stocks/{}/bars'.format(ticker)
        print(endpoint)
        timeout  = aiohttp.ClientTimeout(total=2)
        async with session.get(endpoint,params=payload, headers={ 'APCA-API-KEY-ID': config.API_KEY, 'APCA-API-SECRET-KEY': config.API_SECRET }, timeout=timeout) as response:
            resp = await response.json()
            if resp['bars'] == None:
                return

            bars = pd.DataFrame(resp['bars'])
            bars.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'n', 'vw']
            bars = bars.drop(columns=['n','vw'])
            fmt = '%Y-%m-%d %H:%M:%S'
            bars['Time'] = pd.to_datetime(bars['Time'],format=fmt).dt.tz_localize(None)
            bars = bars.set_index('Time')
            bars = bars.reset_index()
            bars = bars.tail(payload['limit'])
            bars['Ticker'] = ticker
            # print(bars.head(-1))
            return bars.head(-1)

    except Exception as e:
        # print(f"{ticker} : {e}")
        FAIL_STACK.append(ticker)

async def fetch_bars_previous(ticker, payload, session):
    try:

        endpoint = APCA_API_BASE_URL + '/stocks/{}/bars'.format(ticker)
        timeout  = aiohttp.ClientTimeout(total=2)
        async with session.get(endpoint,params=payload, headers={ 'APCA-API-KEY-ID': config.API_KEY, 'APCA-API-SECRET-KEY': config.API_SECRET }, timeout=timeout) as response:
            resp = await response.json()
            if resp['bars'] == None:
                # print(resp)
                return

            bars = pd.DataFrame(resp['bars'])
            bars.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'n', 'vw']
            bars = bars.drop(columns=['n','vw'])
            fmt = '%Y-%m-%d %H:%M:%S'
            bars['Time'] = pd.to_datetime(bars['Time'],format=fmt).dt.tz_localize(None)
            bars = bars.set_index('Time')
            bars = bars.reset_index()
            bars = bars.tail(payload['limit'])
            bars['Ticker'] = ticker
            return bars

    except Exception as e:
        # print(f"{ticker} : {e}")
        FAIL_STACK.append(ticker)


async def fetch_symbol_snapshot(symbols_str, session):
    try:
        # print(symbols_str)
        endpoint = 'https://data.alpaca.markets/v2/stocks/snapshots'
        payload = {
            'symbols' : symbols_str
        } 
        timeout  = aiohttp.ClientTimeout(total=2)
        async with session.get(endpoint,params=payload, headers={ 'APCA-API-KEY-ID': config.API_KEY, 'APCA-API-SECRET-KEY': config.API_SECRET },timeout=timeout) as response:
            resp = await response.text()
    
            return format_snapshot(resp)

    except Exception as e:
        # print(e)
        FAIL_STACK.append(symbols_str)

def format_snapshot(response_content):
    data = json.loads(response_content)
    data = {k: v for k, v in data.items() if v is not None}

    df = pd.json_normalize(list(data.values()))
    df.index = data.keys()
    df['%CLOSE'] = (df['minuteBar.c'] - df['prevDailyBar.c']) / df['prevDailyBar.c'] * 100
    df['%OPEN'] = (df['minuteBar.c'] - df['dailyBar.o']) / df['dailyBar.o'] * 100
    df['%AHRS'] = (df['minuteBar.c'] - df['dailyBar.c']) / df['dailyBar.c'] * 100

    if set(['prevDailyBar' 'latestTrade' 'minuteBar' 'dailyBar']).issubset(set(list(df.columns.values))):
        df = df.drop(columns = ['latestTrade.t', 'latestTrade.x', 'latestTrade.p', 'latestTrade.s',
                                'latestTrade.c', 'latestTrade.i', 'latestTrade.z','latestQuote.c','prevDailyBar',
                                'latestTrade', 'minuteBar', 'dailyBar'],axis=1)
    else:
        df = df.drop(columns = ['latestTrade.t', 'latestTrade.x', 'latestTrade.p', 'latestTrade.s',
                                'latestTrade.c', 'latestTrade.i', 'latestTrade.z','latestQuote.c'],axis=1)


    df['timestamp'] = pd.Series(pd.to_datetime('now')).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')[0]
    df.index.name = 'Ticker'
    df['latestQuote.t'] = pd.to_datetime(df['latestQuote.t'])
    df['minuteBar.t'] = pd.to_datetime(df['minuteBar.t'])
    df['dailyBar.t'] = pd.to_datetime(df['dailyBar.t'])
    df['prevDailyBar.t'] = pd.to_datetime(df['prevDailyBar.t'])

    return df

async def batch_fetcher(all_symbols, async_function, batch_len=1000, payload={}):

    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    num_calls = int(len(all_symbols) / batch_len) + 1

    DF_LIST_TO_CONCAT = []
    for i in tqdm(range(0, num_calls), disable=TQDM_OFF):
    
        symbols = all_symbols[batch_len*i:batch_len*(i+1)]

        async with aiohttp.ClientSession() as session:
            ret = await asyncio.gather(*[async_function(ticker, payload, session) for ticker in symbols][:batch_len])
            print(f'[-] stocks_199_day : {len(ret)} in batch - {i}/{num_calls}')
            DF_LIST_TO_CONCAT += ret
        


    global FAIL_STACK
    if len(FAIL_STACK) != 0:    
        print(f'[-] stocks_200_day : repairing')   
        REPAIR_STACK = FAIL_STACK  
        FAIL_STACK   = []
        REPAIR_OUTPUT = []
        for ticker in REPAIR_STACK:
            # print(f'repairing {ticker}')
            async with aiohttp.ClientSession() as session:
                ret = await async_function(ticker, payload, session)
                # print(ret)
                REPAIR_OUTPUT.append(ret)

        DF_LIST_TO_CONCAT += REPAIR_OUTPUT

    # print(REPAIR_OUTPUT)
    # for item in DF_LIST_TO_CONCAT: print(type(item))
    print('[-] stocks_200_day : <end>')

    return pd.concat([*DF_LIST_TO_CONCAT],axis=0)

async def batch_fetcher_snapshot(all_symbols, async_function,chunk_size=1000):

    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    num_calls = int(len(all_symbols) / chunk_size) + 1

    DF_LIST_TO_CONCAT = []
    for i in tqdm(range(0, num_calls), disable=TQDM_OFF):

        symbols = all_symbols[chunk_size*i:chunk_size*(i+1)]

        async with aiohttp.ClientSession() as session:
            ret = await asyncio.gather(*[async_function(ticker, session) for ticker in symbols][:chunk_size])
            DF_LIST_TO_CONCAT += ret
    print('[-] stocks_snapshot : <end>')
    return pd.concat([*DF_LIST_TO_CONCAT],axis=0).reset_index() #FIXME: 
    
def async_helper(symbols, function_ref, batch_len, payload={}):
    loop = asyncio.get_event_loop()
    task = loop.create_task(batch_fetcher(symbols, function_ref, batch_len, payload))

    return (loop.run_until_complete(task))

def async_helper_snapshot(symbols, function_ref):
    loop = asyncio.get_event_loop()
    task = loop.create_task(batch_fetcher_snapshot(symbols, function_ref))

    return (loop.run_until_complete(task))

def fetch_snapshot(symbols=None):
    print('[-] stocks_snapshot : <start>')

    if symbols == None:
        symbols = fetch_active_symbols()

    symbol_str_lists = [','.join(symbols[i:i + 1000]) for i in range(0, len(symbols), 1000)]

    return async_helper_snapshot(symbol_str_lists, fetch_symbol_snapshot).drop(['prevDailyBar', 'minuteBar',  'dailyBar'],axis=1)

def fetch_200d_snapshot(symbols=None):
    
    print('[-] stocks_200_day : <start>')


    if symbols == None:
        symbols = fetch_active_symbols()    
    
    return async_helper(symbols, fetch_bars, batch_len=100, payload = {
        'start'     : fetch_start_end_period(200)['start'],
        'end'       : fetch_start_end_period(200)['end'],
        'timeframe' : '1Day',
        'limit'     : 200,
    })

def fetch_start_end_period(limit=200):

    today = str(dt.datetime.today().date())
    dates = fetch_dates(end=today).tail(limit)

    start_date = dates.iloc[0]['date']
    end_date   = dates.iloc[-1]['date']

    start : dt = dt.datetime.strptime(start_date,"%Y-%m-%d")
    end   : dt = dt.datetime.strptime(end_date,"%Y-%m-%d") + dt.timedelta(days=1)

    #TODO: if market is open + want daily bar, add one day to end

    fmt   : str = '%Y-%m-%dT%H:%M:%SZ'
    end   : dt = end.astimezone().strftime('%Y-%m-%dT%H:%M:%SZ')
    start : dt = start.astimezone().strftime('%Y-%m-%dT%H:%M:%SZ')

    return { 'start': start, 'end': end }


def fetch_dates(start='',end=''):
    payload = {'start': start, 'end': end}
    response = requests.get(url="http://api.alpaca.markets/v2/calendar", headers={ 'APCA-API-KEY-ID': config.API_KEY, 'APCA-API-SECRET-KEY': config.API_SECRET }, params = payload, timeout=1)

    return pd.DataFrame(response.json())


def fetch_margin_snapshot(exchange=None,symbols=None):

    response = requests.get(url='http://api.alpaca.markets' + '/v2/assets', headers={ 'APCA-API-KEY-ID': config.API_KEY, 'APCA-API-SECRET-KEY': config.API_SECRET }, timeout=1)
    var = pd.DataFrame(response.json()).set_index('symbol')

    if symbols != None:
        var = var[var.index.isin(symbols)]

    var = var[var['status']  == 'active']
    var = var.drop(columns = ['id', 'status', 'fractionable', 'class'],axis=1)

    if exchange == 'NASDAQ':
        return [a for a in var if a['exchange'] == 'NASDAQ']
    elif exchange == 'NYSE':
        return [a for a in var if a['exchange'] == 'NYSE']
    else:
        return var[(var['exchange'] =='NASDAQ') | (var['exchange'] == 'NYSE')]

def fetch_active_symbols():
    return list(fetch_margin_snapshot().index.values)


def cache_199_day_bars():
    fetch_200d_snapshot().to_pickle('__cache__/bars_199_day')

def cache_200_day_bars():
    bars_snapshot = pd.read_pickle('__cache__/bars_199_day')

    stocks_snapshot = fetch_snapshot()
    stocks_snapshot.to_pickle('__cache__/stock_snapshot')
    active_symbols = bars_snapshot['Ticker'].unique()

    stocks_snapshot = stocks_snapshot[stocks_snapshot['Ticker'].isin(active_symbols)].reset_index()
    stocks_snapshot = stocks_snapshot[['dailyBar.t', 'dailyBar.o', 'dailyBar.h', 'dailyBar.l', 'dailyBar.c', 'dailyBar.v', 'Ticker']]
    stocks_snapshot.columns=['Time','Open','High','Low','Close','Volume','Ticker']
    
    updated_bars = pd.concat([bars_snapshot,stocks_snapshot],axis=0)
    updated_bars.to_pickle('__cache__/bars_200_day')

    # print(updated_bars.query("Ticker == 'AAPL'"))
    print('[-] bars_200_day : ', len(updated_bars.query("Ticker == 'AAPL'")))

    return updated_bars

def fetch_option_symbols():
    cboe = pd.read_csv(\
        'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt=cone'\
            )
    cboe['Exchange'] = 'Cboe'
    bzx = pd.read_csv(\
        'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt=opt'\
            )
    bzx['Exchange'] = 'BZX'
    # c2 = pd.read_csv(\
    #     'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt=ctwo'\
    #         )
    # c2['Exchange'] = 'C2'
    # edgx = pd.read_csv(\
    #     'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt=exo'\
    #         )
    # edgx['Exchange'] = 'EDGX'

    
    options = list( pd.concat([cboe,bzx],axis=0)['Symbol'].unique() )
    return list( set(fetch_active_symbols()).intersection(set(options)) )
