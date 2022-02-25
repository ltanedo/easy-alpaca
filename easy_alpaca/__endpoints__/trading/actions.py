import requests

from ...__utility__ import config 
from ...__utility__ import fetcher

def get(ca_types, since, until):
    endpoint = config.TRADING_URL + '/corporate_actions/announcements'
    params   = {
        "ca_types" : ca_types,
        "since"    : since,
        "until"    : until
    }
    response = fetcher.fetch(
                    url=endpoint,
                    params=params
                )

    return response


