import requests
import datetime as dt
import pandas as pd
import asyncio

from ...__utility__ import config 
from ...__utility__ import fetcher

def snapshot(LATEST=True):

    if not LATEST:
        return {"error" : "only LATEST news implemented"}

    endpoint = 'https://data.alpaca.markets/v1beta1/news'
    response = fetcher.fetch(
                    url=endpoint,
                    params={}
                )

    return pd.DataFrame(response)

def get(ticker):
    endpoint = 'https://data.alpaca.markets/v1beta1/news'
    response = fetcher.fetch(
                    url=endpoint,
                    params={
                        'symbols' : ticker
                    }
                )

    return pd.DataFrame(response)
