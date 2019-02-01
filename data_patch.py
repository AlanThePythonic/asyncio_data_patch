#!/usr/bin/python3
import pymongo
import http.client
import json
from datetime import datetime
import asyncio
import aiohttp
import sys
import getopt
from multiprocessing import Process


SOURCE_URL = "https://min-api.cryptocompare.com"
HISTOMINUTE = "histominute"
HISTOHOUR = "histohour"
HISTODAY = "histoday"
METHOD = 'GET'
MINUTE_BUFFER = 1440
HOUR_BUFFER = 24
DATABASE = 'market_data'


def truncate_collections(db_client, collections, quote, base):
    db = db_client[DATABASE]
    for col in collections:
        tmp = db[col].delete_many({"fromSymbol": quote, "toSymbol": base})
        print("[{}] - {} documents deleted.".format(base +
                                                    quote, tmp.deleted_count), end="\n")


async def http_request(url, postfix, params, method):
    url = SOURCE_URL + '/data/{}{}'.format(postfix, params)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return json.loads(await resp.text())


async def patch_minute_data(db_client, fsym, tsym, limit, toTs=None, aggregate=1, result=None):
    print("\n[Minutely] Starting to patch data of pairs : {}{}".format(fsym, tsym))
    if limit > 10080:
        limit = 10080
    db = db_client[DATABASE]
    minute_data_collection = db["hist_minute"]
    if limit <= MINUTE_BUFFER:
        postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
            fsym, tsym, limit, aggregate)
        if toTs != None:
            postfix += "&toTs={}".format(toTs)
        data = await (http_request(SOURCE_URL, HISTOMINUTE, postfix, METHOD))
        data = data["Data"]
        for obj in data:
            obj["fromSymbol"] = tsym
            obj["toSymbol"] = fsym
            obj["volumeto"] = 0
        data.pop()
        toTs = data[0]['time']
        minute_data_collection.insert_many(data)
        await asyncio.sleep(1)
        print("[Minutely] Patched : {} -> {} ".format(datetime.utcfromtimestamp(int(toTs)).strftime('%Y-%m-%d %H:%M:%S'),
                                                      datetime.utcfromtimestamp(int(data[-1]['time'])).strftime('%Y-%m-%d %H:%M:%S')))
    else:
        times = int(limit / MINUTE_BUFFER)
        for n in range(0, times):
            postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
                fsym, tsym, MINUTE_BUFFER, aggregate)
            if toTs != None:
                postfix += "&toTs={}".format(toTs)
            data = None
            while True:
                try:
                    data = await (http_request(SOURCE_URL, HISTOMINUTE, postfix, METHOD))
                    data = data["Data"]
                    if len(data) > 0:
                        break
                except Exception as e:
                    print("retrying ...  {}".format(e))
            for obj in data:
                obj["fromSymbol"] = tsym
                obj["toSymbol"] = fsym
                obj["volumeto"] = 0
            if n != 0:
                data.pop()
            toTs = data[0]['time']
            minute_data_collection.insert_many(data)
            await asyncio.sleep(1)
            print("[Minutely] Patched : {} -> {} ".format(datetime.utcfromtimestamp(int(toTs)).strftime('%Y-%m-%d %H:%M:%S'),
                                                          datetime.utcfromtimestamp(int(data[-1]['time'])).strftime('%Y-%m-%d %H:%M:%S')))


async def patch_hour_data(db_client, fsym, tsym, limit, toTs=None, aggregate=1):
    print("\n[Hourly] Starting to patch data of pairs : {}{}".format(fsym, tsym))
    if limit > 86400:
        limit = 86400
    db = db_client[DATABASE]
    hour_data_collection = db["hist_hour"]
    if limit <= HOUR_BUFFER:
        postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
            fsym, tsym, limit, aggregate)
        if toTs != None:
            postfix += "&toTs={}".format(toTs)
        data = await (http_request(SOURCE_URL, HISTOHOUR, postfix, METHOD))
        data = data["Data"]
        data.pop()
        toTs = data[0]['time']
        hour_data_collection.insert_many(data)
        await asyncio.sleep(1)
        print("[Hourly] Patched : {} -> {} ".format(datetime.utcfromtimestamp(int(toTs)).strftime('%Y-%m-%d %H:%M:%S'),
                                                    datetime.utcfromtimestamp(int(data[-1]['time'])).strftime('%Y-%m-%d %H:%M:%S')))
    else:
        # Patch per 2 months of hour 's candle stick
        times = 2
        for n in range(0, times):
            postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
                fsym, tsym, 744, aggregate)
        # times = int(limit / HOUR_BUFFER / 60)
        # for n in range(0, times):
        #     postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
        #         fsym, tsym, HOUR_BUFFER, aggregate)
            if toTs != None:
                postfix += "&toTs={}".format(toTs)
            data = None
            while True:
                try:
                    data = await (http_request(SOURCE_URL, HISTOHOUR, postfix, METHOD))
                    data = data["Data"]
                    if len(data) > 0:
                        break
                except Exception as e:
                    print("retrying ...  {}".format(e))
            for obj in data:
                obj["fromSymbol"] = tsym
                obj["toSymbol"] = fsym
                obj["volumeto"] = 0
            if n != 0:
                data.pop()
            toTs = data[0]['time']
            hour_data_collection.insert_many(data)
            await asyncio.sleep(1)
            print("[Hourly] Patched : {} -> {} ".format(datetime.utcfromtimestamp(int(toTs)).strftime('%Y-%m-%d %H:%M:%S'),
                                                        datetime.utcfromtimestamp(int(data[-1]['time'])).strftime('%Y-%m-%d %H:%M:%S')))


async def patch_day_data(db_client, fsym, tsym, limit, toTs=None, aggregate=1):
    print("\n[Daily] Starting to patch data of pairs : {}{}".format(fsym, tsym))
    db = db_client[DATABASE]
    day_data_collection = db["hist_day"]
    limit = limit / 60 / 24
    postfix = "?fsym={}&tsym={}&limit={}&aggregate={}".format(
        fsym, tsym, limit, aggregate)
    if toTs != None:
        postfix += "&toTs={}".format(toTs)
    data = []
    while True:
        try:
            data = await (http_request(SOURCE_URL, HISTODAY, postfix, METHOD))
            data = data["Data"]
            break
        except Exception as e:
            print("retrying ...  {}".format(e))
    for obj in data:
        obj["fromSymbol"] = tsym
        obj["toSymbol"] = fsym
        obj["volumeto"] = 0
    if len(data) >= 0 and data != None:
        try:
            data.pop()
        except Exception:
            pass        
    day_data_collection.insert_many(data)
    await asyncio.sleep(1)
    toTs = data[0]['time']
    print("[Daily] Patched : {} -> {} ".format(datetime.utcfromtimestamp(int(toTs)).strftime('%Y-%m-%d %H:%M:%S'),
                                               datetime.utcfromtimestamp(int(data[-1]['time'])).strftime('%Y-%m-%d %H:%M:%S')))


async def patch_process(loop, db_client, quote, base, limit, truncate_all, env):
    quote = quote.upper()
    base = base.upper()
    if truncate_all == 'true':
        collections = ["hist_minute", "hist_hour", "hist_day"]
        truncate_collections(db_client, collections, quote, base)
        print("\nTruncated collections : {}".format(collections), end="\n")
    print("\nStarting to patch historical data by minutely, hourly and daily ... ", end="\n")    
    tasks = [await patch_minute_data(db_client, base, quote, limit),
                await patch_hour_data(db_client, base, quote, limit),
                await patch_day_data(db_client, base, quote, limit)]
    return tasks  


def add_task(loop, db_client, quote, base, limit, truncate_all, env):
    return loop.create_task(patch_process(loop, db_client, quote, base, limit, truncate_all, env))


def process(quote, base, limit, truncate_all, env):    
    with open('../config/config_{}.json'.format(env)) as f:
        data = json.load(f)
        mongo_url = data["mongodb"]["mongodb"]
        db_client = None
        try:
            url_separate = mongo_url.split("/")
            login_info = url_separate[2]
            auth_info = login_info.split('@')
            id_and_pw = auth_info[0].split(':')
            ip_and_port = auth_info[1]
            database_and_authSource = url_separate[3].split("?")
            database_and_authSource[1] = database_and_authSource[1].split('=')[1]
            db_client = pymongo.MongoClient("mongodb://" + ip_and_port)
            db_client[database_and_authSource[1]].authenticate(id_and_pw[0], id_and_pw[1], mechanism="SCRAM-SHA-1")
        except Exception as e:
            print("Error of reading configuration json file : {}, now setup directly by mongo URL".format(e))
            db_client = pymongo.MongoClient(mongo_url)
        loop = asyncio.get_event_loop()
        task_list = []
        for currency in data['server']['currencies']:
            if currency['available'] == True:
                print(currency)                
                task_list.append(add_task(loop, db_client, currency['quote'], currency['base'], limit, truncate_all, env))                
        loop.run_until_complete(asyncio.wait(task_list))
        loop.close()
        db_client.close()


def exception():
    print('Usage : data_patch.py -q <quote> -b <base> -l <limit> -t <truncate>')
    sys.exit()


def main(argv):
    quote = ''
    base = ''
    limit = 0
    truncate_all = ''
    env = ''
    try:
        opts, args = getopt.getopt(
            argv, "h:q:b:l:t:e:", ["help", "quote=", "base=", "limit=", "truncate=", "env="])
        if len(opts) != 5:
            exception()
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                exception()
            elif opt in ("-q", "--quote"):
                quote = arg.upper()
            elif opt in ("-b", "--base"):
                base = arg.upper()
            elif opt in ("-l", "--limit"):
                limit = int(arg)
            elif opt in ("-t", "--truncate"):
                truncate_all = arg.lower()
            elif opt in ("-e", "--env"):
                env = arg.lower()
    except getopt.GetoptError:
        exception()
    process(quote, base, limit, truncate_all, env)


if __name__ == '__main__':
    main(sys.argv[1:])
