import requests as req
import re
import time
import datetime
util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe

timeStamp = round(time.time()*1000)
currentDateTime = time.localtime()
currentDate = time.strftime('%Y-%m-%d', currentDateTime)

def refresh_k_line_hour(code):
    url = 'https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_' + code + '_60_' + str(timeStamp) + '=/CN_MarketDataService.getKLineData?symbol=sh600031&scale=60&ma=no&datalen=1023'
    content = req.get(url).text
    #pattern = re.compile(r'\\{ *"day":"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d" *, ')
    pattern = re.compile(r'[\\{].+?[\\}]')
    patternTime = re.compile(r'\d\d\d\d-\d\d-\d\d +\d\d:\d\d:\d\d')
    patternPrice = re.compile(r'\d+[\\.]+\d+')
    patternVolume = re.compile(r'\d\d\d\d\d\d*')
    result = pattern.findall(content)
    time = ''
    open = ''
    high = ''
    low = ''
    volume = '0'
    i = len(result) - 1
    while (i >= 0):
        item = result[i]
        resultTime = patternTime.findall(item)
        if (len(resultTime)>0):
            time = str(resultTime[0])
            klineDate = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
            #if (currentDate != klineDate.strftime('%Y-%m-%d')):
            #    break
        resultPrice = patternPrice.findall(item)
        resultVolume = patternVolume.findall(item)
        if (len(resultVolume) >= 1):
            volume = str(resultVolume[0])

        if (len(resultPrice) >= 4):
            open = str(resultPrice[0])
            high = str(resultPrice[1])
            low = str(resultPrice[2])
            close = str(resultPrice[3])
            refresh_k_line(code, time, open, high, low, close, volume)
        i = i - 1
    pipe.execute()
    return result

def refresh_k_line(code, time, open, high, low, close, volume):
    str_key_name = code + '_kline_hour'
    timestamp = util.get_timestamp(time, '%Y-%m-%d %H:%M:%S')
    value_str = code + ',' + time + ',' + open + ',' + close + ',' + high + ',' + low + ',' + volume + ',0'
    pipe.zremrangebyscore(str_key_name, timestamp, timestamp)
    pipe.zadd(str_key_name, {value_str: timestamp})
    pipe.persist(str_key_name)
    return

#refresh_k_line_hour('sh600031')

all_gids = redis.smembers('all_gids')

while (len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)
    refresh_k_line_hour(str_code)