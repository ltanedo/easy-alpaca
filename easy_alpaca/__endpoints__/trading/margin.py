import requests
import pandas as pd 

from ...__utility__ import config 
from ...__utility__ import fetcher

def snapshot():
    endpoint = config.TRADING_URL + '/assets'
    response = fetcher.fetch(
                    url=endpoint,
                    params={}
                )

    return pd.DataFrame(response)