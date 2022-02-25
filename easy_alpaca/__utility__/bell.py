import datetime as dt
import pytz     as pytz
import time     as time

tz : pytz.timezone = pytz.timezone('America/New_York')

def get_market_time():
    return dt.datetime.now(tz)

def cooldown():
    wait_seconds = 60 - dt.datetime.now().second
    time.sleep(wait_seconds)
    print('[+1] ', dt.datetime.now(tz))


def wait_until_open():

    if dt.datetime.now(tz).time() > dt.time(20,00,00):
        print("[-] market evening : closing")
        return

    DATE_TODAY : dt = dt.datetime.today()
    OPEN_TIME  : dt = dt.datetime(DATE_TODAY.year, DATE_TODAY.month, DATE_TODAY.day,9,30).replace(tzinfo=tz)
    while ( dt.datetime.now(tz) < OPEN_TIME ) : cooldown()
    return


def is_market_open():

    if dt.datetime.now(tz).time() > dt.time(20,00,00):
        print("[-] market evening : closing")
        return False

    return dt.time(8,59,0) < dt.datetime.now(tz).time() and dt.datetime.now(tz).time() < dt.time(20,0,0)


def is_open_stable():

    if dt.datetime.now(tz).time() > dt.time(20,00,00):
        print("[-] market evening : closing")
        return False

    return dt.time(10,10,0) < dt.datetime.now(tz).time() and dt.datetime.now(tz).time() < dt.time(20,0,0)  

def get_difference(time1, time2):
    return (time2-time1).total_seconds()