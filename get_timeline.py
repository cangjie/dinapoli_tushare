import datetime
import threading
import tushare as ts
import sys
import urllib
import json

util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe
token = '4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec'
ts.set_token(token)
pro = ts.pro_api()
api = ''
gidArr = []
if (len(sys.argv) == 2):
    api = sys.argv[1]

if (api == ''):
    gidArr = redis.smembers('all_gids')
else:
    currentDate = datetime.datetime.now()
    if (not (util.is_trans_day(datetime.datetime.strftime(currentDate, '%Y%m%d')))):
        exit(0)
    current_date = util.get_last_trans_day(datetime.datetime.strftime(currentDate, '%Y%m%d'), 1)
    currentDate = datetime.datetime.strptime(current_date, '%Y%m%d')
    current_date = datetime.datetime.strftime(currentDate, '%Y-%m-%d')
    url = 'http://weixin.goldenma.xyz/api/' + api + '/0?startDate=' +  current_date + '&endDate=' + current_date
    req = urllib.request.urlopen(url)
    ret = req.read()
    jsonStr = ret.decode('utf-8')
    json = json.loads(jsonStr)
    i = 0
    itemList = json['itemList']
    while (i < len(itemList)):
        gidArr.append(itemList[i]['gid'])
        i = i + 1


def get_tick(gid):
    print(gid + ' start')
    newGid = gid[2:8] + '.' + gid[0:2].upper()
    df = ts.realtime_tick(ts_code = newGid)
    key = gid + '_tick'
    redis.delete(key)
    i = 0
    while (i < df.index.size):
        type = 'E'
        if (df['TYPE'][i] == '买盘'):
            type = 'U'
        if (df['TYPE'][i] == '卖盘'):
            type = 'D'
        valueStr = df['TIME'][i] + ',' + str(df['PRICE'][i]) + ',' + str(df['CHANGE'][i]) + ',' + str(df['VOLUME'][i]) + ',' + str(df['AMOUNT'][i]) + ',' + type
        pipe.zadd(key, {valueStr: i})
        i = i + 1
    #print(df)
    try:
        pipe.persist(key)
        pipe.execute()
    except Exception as err:
        print(str(err))
    finally:
        print(gid + ' end')



#get_tick('sh600031')

threads = []
while (len(gidArr) > 0):
    gid = gidArr.pop()
    t = threading.Thread(target=get_tick, args=(gid,))
    threads.append(t)
if __name__ == '__main__':
    for t in threads:
        t.start()
    for t in threads:
        t.join()

