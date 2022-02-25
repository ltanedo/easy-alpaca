import requests

from ...__utility__ import config 
from ...__utility__ import fetcher

def get():
    endpoint = config.TRADING_URL + '/calendar'
    response = fetcher.fetch(
                    url=endpoint,
                    params={}
                )

    return response


