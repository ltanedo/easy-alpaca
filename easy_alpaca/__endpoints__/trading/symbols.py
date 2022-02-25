import requests
import pandas as pd 

from ...__utility__ import config 
from ...__utility__ import fetcher

from ..trading import margin

def get(EXCHANGES=[]):
    margin_snap = margin.snapshot().query("status == 'active'")
    if EXCHANGES:
        margin_snap = margin_snap[ margin_snap["exchange"].isin(EXCHANGES) ]

    return list(margin_snap.symbol)