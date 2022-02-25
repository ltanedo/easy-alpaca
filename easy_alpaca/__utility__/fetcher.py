import requests as requests
import pandas   as pd
import asyncio
import aiohttp

from .. import config

FAILED_SYMBOLS = []
    
def fetch(url, params, timeout=3, ASYNC=False):
    if ASYNC:
        loop = asyncio.get_event_loop()
        task = loop.create_task(
                fetch_async(url, params, timeout)
            )
        return loop.run_until_complete(task)
    
    return fetch_sync_helper(url, params, timeout)

def fetch_sync_helper(url, params, timeout=3, retries=3):
    counter = 0
    error_str = ''
    while counter < retries:
        try:
            # config.UPDATE_RATE_LIMIT()
            response = requests.get(url, 
                                    params, 
                                    headers={ 
                                        'APCA-API-KEY-ID':     config.API_KEY, 
                                        'APCA-API-SECRET-KEY': config.API_SECRET
                                    },
                                    timeout=timeout
                                    )
            data = response.json() \
                if   config.PD_OFF == True \
                else pd.DataFrame(response.json()).T
            
            return data

        except Exception as e:
            counter += 1
            error_str = str(e)

    return {
        "error" : error_str
    }


async def fetch_async(url, params, timeout):
    async with aiohttp.ClientSession() as session:
        return await fetch_async_helper(url, params, timeout, session)

async def fetch_async_helper(url, params, session, ticker='', timeout=2):
    try:
        timeout  = aiohttp.ClientTimeout(total=timeout)
        headers  =  { 
                        'APCA-API-KEY-ID':     config.API_KEY, 
                        'APCA-API-SECRET-KEY': config.API_SECRET
                    }
        async with session.get(url, params=params, headers=headers, timeout=timeout) \
            as response:
                resp = await response.json()
                
                if 'bars' in resp and resp['bars'] == None:
                    return pd.DataFrame(resp, index=[0])

        # return pd.DataFrame(resp)
        return pd.json_normalize(resp)


    except Exception as e:
       print(url, e)
       return pd.DataFrame({'symbol' : ticker, 'error' : e},index=[0])

def response_parser(resp, ticker):
    if 'bars' in resp:
        if resp['bars'] == None:
            temp = {
                'ticker' : ticker,
                'error'  : True
            }
            return pd.DataFrame(temp, index=[0])
        else: 
            frame = pd.DataFrame(resp['bars'])
            frame['ticker'] = ticker
            return pd.DataFrame(frame)
    
    return pd.DataFrame(resp)

async def batch_fetcher(all_symbols, endpoint, payload={}, BATCH_LEN=1000):

    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    if "{}" in endpoint:
        # all_symbols = [(endpoint.replace("{}", ticker),ticker) for ticker in all_symbols]
        all_symbols = [ ( endpoint.replace("{}", ticker), ticker ) for ticker in all_symbols]

    NUM_CALLS = int(len(all_symbols) / BATCH_LEN) + 1
    DF_LIST_TO_CONCAT = []
    for i in range(0, NUM_CALLS):
    
        SUBSET_SYMBOLS = all_symbols[BATCH_LEN*i:BATCH_LEN*(i+1)]
        async with aiohttp.ClientSession() as session:
            ret = await asyncio.gather(*[fetch_async_helper(ticker[0], payload, session, ticker[1]) for ticker in SUBSET_SYMBOLS][:BATCH_LEN])
            print(f'[-] stocks_199_day : {len(ret)} in batch - {i}/{NUM_CALLS}')
            DF_LIST_TO_CONCAT += ret
    
    print('[-] stocks_200_day : <end>')

    return pd.concat([*DF_LIST_TO_CONCAT],axis=0)
