import requests
import datetime as dt
import pandas as pd
import asyncio

from ...__utility__ import config 
from ...__utility__ import fetcher

def get(ticker, start="null", end="null", limit=200, adjustment="null"):
    endpoint = config.DATA_URL + f"/stocks/{ticker}/bars"
    period   = fetch_start_end_period(start,end,limit)
    payload = { 'start'     : period['start'],
                'end'       : period['end'], 
                'timeframe' : '1Day',
                'limit'     : limit,
            }
                            
    VALID = [ 
        "--",      #  None 
        "raw",          
        "split", 
        "dividend", 
        "all"
    ]
    if adjustment != "null" and adjustment in VALID: 
        payload['adjustment'] = adjustment

    response = fetcher.fetch(
                    url=endpoint,
                    params=payload
                )

    return pd.DataFrame(response)

def fetch_dates(start='',end=''):
    payload = {'start': start, 'end': end}
    response = requests.get(url="http://api.alpaca.markets/v2/calendar", headers={ 
                                    'APCA-API-KEY-ID':     config.API_KEY, 
                                    'APCA-API-SECRET-KEY': config.API_SECRET
                                }, params = payload, timeout=1)
    return pd.DataFrame(response.json())

def fetch_start_end_period(START="null",END="null", LIMIT=200):

    if START == "null" and END == "null":
        today = str(dt.datetime.today().date())
        dates = fetch_dates(end=today).tail(LIMIT)
    elif START != "null" and END != "null":
        today = str(dt.datetime.today().date())
        dates = fetch_dates(end=today).tail(LIMIT)
    elif END != "null":
        today = END
        dates = fetch_dates(end=today).tail(LIMIT)
    elif START != "null":
        today = START
        dates = fetch_dates(start=today).head(LIMIT)
    else:
        raise Exception("START and END invalid")

    start_date = dates.iloc[0]['date']
    end_date   = dates.iloc[-1]['date']

    start : dt = dt.datetime.strptime(start_date,"%Y-%m-%d")
    end   : dt = dt.datetime.strptime(end_date,"%Y-%m-%d") + dt.timedelta(days=1)

    #TODO: if market is open + want daily bar, add one day to end

    fmt   : str = '%Y-%m-%dT%H:%M:%SZ'
    end   : dt = end.astimezone().strftime('%Y-%m-%dT%H:%M:%SZ')
    start : dt = start.astimezone().strftime('%Y-%m-%dT%H:%M:%SZ')

    return { 'start': start, 'end': end }

def snapshot_helper(symbols):

    from ...__utility__ import bell
    bell.cooldown()

    if not symbols:
        from ..trading import symbols
        symbols = symbols.get(["NYSE","NASDAQ"])

    endpoint = config.DATA_URL + "/stocks/{}/bars"
    period   = fetch_start_end_period()
    payload = { 'start'     : period['start'],
                'end'       : period['end'], 
                'timeframe' : '1Day',
                'limit'     : 200
            }
    loop = asyncio.get_event_loop()
    task = loop.create_task(
            fetcher.batch_fetcher(symbols, endpoint, payload)
        )
        
    return loop.run_until_complete(task)         

def snapshot(symbols = [], HISTORICAL=False):

    if HISTORICAL == False:
        return realtime()

    counter = 0 
    retries = 3

    while counter < retries:
        result = snapshot_helper(symbols)
        if 'error' not in result.columns:

            final=[]
            for ticker in list(result.symbol):
                subset = result[ result.symbol==ticker ]
                temp = pd.DataFrame(result['bars'].values[0])
                temp['ticker'] = ticker
                final.append(temp)

            return pd.concat(final, axis=0)

    return {"error" : "snapshot failed with errors"}

def realtime(symbols = []):

    from . import old_snapshot

    if not symbols:
        from ..trading import symbols
        symbols = symbols.get(["NYSE","NASDAQ"])

    return old_snapshot.fetch_snapshot(symbols)