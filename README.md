# easy-alpaca
A declarative python library for fetching market data from the NYSE and NASDAQ. 
- Uses pandas by default
- Uses asyncio and aiohttp for large async requests

## Initial Setup 
```
import easy_alpaca as alpaca

alpaca.config.API_KEY    = ("ALPACA_API")
alpaca.config.API_SECRET = ("ALPACA_SECRET")
```

## 200_Day_Bars Snapshot
```
df = alpaca.bars.snapshot(
        HISTORICAL=True
    )
```

## Single_PrevDay_Day_Min Snapshot
```
df = alpaca.bars.snapshot(
        HISTORICAL=False
    )
```

## Daily Bars 
> Warning: only start or end accepted (NOT BOTH)
> Warning: limit only tested up to 365 (in days)
```
df = alpaca.bars.get(
        ticker     = 'NVDA', 
        start      = '2020-09-23',
        end        = 'null',
        limit      = 100,
        adjustment = 'all',
    )
```

## Latest News for all stocks
```
df = alpaca.news.snapshot(
        LATEST=True
    )
```
## Latest News for single stock
```
df = alpaca.news.get('AAPL')
```
