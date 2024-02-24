import datetime
import threading
import tushare as ts
import sys
import urllib
import json
import time
util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe
token = '4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec'
ts.set_token(token)
pro = ts.pro_api()
api = ''
gidArr = []
threadNum = 30
#nowDate = datetime.datetime.now() + datetime.timedelta(days=-1)
nowDate = datetime.datetime.now()
if (not util.is_trans_day(nowDate.strftime('%Y%m%d'))):
    exit(0)

if (len(sys.argv) >= 2):
    api = sys.argv[1]
if (len(sys.argv) >= 3):
    threadNum = int(sys.argv[2])

if (api == ''):
    gidArrOri = redis.smembers('all_gids')
    i = 0
    gidArr = []
    for i in range(0, len(gidArrOri)):
        gid = gidArrOri.pop().decode('utf-8').split(' ')[0]
        gidArr.append(gid)
elif (api.find('/') < 0 ):
    gidArrAll = redis.smembers('all_gids')
    i = 0
    for i in range(0, len(gidArrAll)):
        gid = gidArrAll.pop().decode('utf-8').split(' ')[0]
        if (gid.startswith(api)):
            gidArr.append(gid)
else:
    #currentDate = datetime.datetime.now() + datetime.timedelta(days=-1)
    currentDate = datetime.datetime.now()
    if (not (util.is_trans_day(datetime.datetime.strftime(currentDate, '%Y%m%d')))):
        exit(0)
    current_date = util.get_last_trans_day(datetime.datetime.strftime(currentDate, '%Y%m%d'), 1)
    currentDate = datetime.datetime.strptime(current_date, '%Y%m%d')
    current_date = datetime.datetime.strftime(currentDate, '%Y-%m-%d')
    url = 'http://weixin.goldenma.xyz/api/' + api
    req = urllib.request.urlopen(url)
    ret = req.read()
    jsonStr = ret.decode('utf-8')
    json = json.loads(jsonStr)
    i = 0
    itemList = json['itemList']
    while (i < len(itemList)):
        gidArr.append(itemList[i]['gid'])
        i = i + 1

threadNum = min(threadNum, len(gidArr))

class Result:
    def __init__(self):
        self.gid = ''
        self.lines = []
        self.linesToSave = []
        self.try_times = 0
        self.finish = False
        self.saved = False
        self.computing = False

def deal_result(result):
    print(result.gid + ' compute start\r\n')
    result.linesToSave = []
    result.computing = True
    lines = result.lines
    key = result.gid + '_money'
    startScore = util.get_timestamp(nowDate.strftime('%Y%m%d'), '%Y%m%d')
    endScore = util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 23:59:59', '%Y%m%d %H:%M:%S')
    tickArr = redis.zrangebyscore(key, startScore, endScore)
    #tickArr = redis.zrange
    j = 0
    for j in range(0, len(tickArr)):
        tick = tickArr[j].decode('utf-8')

        pipe.zrem(key, tickArr[j])

    hugeDeal = 0
    hugeNetFlow = 0
    bigDeal = 0
    bigNetFlow = 0
    midDeal = 0
    midNetFlow = 0
    smallDeal = 0
    smallNetFlow = 0
    lastLineTimeStamp = 0
    j = 0
    for j in range(0, len(lines)):
        lineArr = lines[j].split(',')
        if (len(lineArr) != 6):
            continue
        timeStr = nowDate.strftime('%Y%m%d') + ' ' + lineArr[0]
        currentLineTimeStamp = util.get_timestamp(timeStr, '%Y%m%d %H:%M:%S')
        needSave = False
        saveTimeStamp = 0

        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 09:30:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 09:30:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 09:30:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')
        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 10:00:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 10:00:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 10:00:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')
        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 10:30:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 10:30:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 10:30:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')
        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 11:00:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 11:00:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 11:00:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')
        if (lastLineTimeStamp <= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 11:30:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp > util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 11:30:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 11:30:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')

        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 13:30:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 13:30:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 13:30:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')

        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 14:00:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 14:00:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 14:00:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')

        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 14:30:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 14:30:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 14:30:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')
        if (lastLineTimeStamp < util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 15:00:00', '%Y%m%d %H:%M:%S') \
                and currentLineTimeStamp >= util.get_timestamp(nowDate.strftime('%Y%m%d') + ' 15:00:00',
                                                               '%Y%m%d %H:%M:%S')):
            needSave = True
            saveTimeStr = nowDate.strftime('%Y-%m-%d 15:00:00')
            saveTimeStamp = util.get_timestamp(saveTimeStr, '%Y-%m-%d %H:%M:%S')

        if (needSave):
            needSave = False
            saveStr = saveTimeStr + ',' + str(hugeDeal) + ',' + str(hugeNetFlow) + ','
            saveStr = saveStr + str(bigDeal) + ',' + str(bigNetFlow) + ',' + str(midDeal) + ',' + str(midNetFlow) + ','
            saveStr = saveStr + str(smallDeal) + ',' + str(smallNetFlow)
            #pipe.zadd(key, {saveStr: saveTimeStamp})
            result.linesToSave.append(saveStr)
            hugeDeal = 0
            hugeNetFlow = 0
            bigDeal = 0
            bigNetFlow = 0
            midDeal = 0
            midNetFlow = 0
            smallDeal = 0
            smallNetFlow = 0
            #pipe.zadd(key, {saveStr: saveTimeStamp})

        volume = int(lineArr[3])
        if (volume >= 2000):
            hugeDeal = hugeDeal + volume
            if (lineArr[5] == 'U'):
                hugeNetFlow = hugeNetFlow + volume
            if (lineArr[5] == 'D'):
                hugeNetFlow = hugeNetFlow - volume
        elif (volume >= 600):
            bigDeal = bigDeal + volume
            if (lineArr[5] == 'U'):
                bigNetFlow = bigNetFlow + volume
            if (lineArr[5] == 'D'):
                bigNetFlow = bigNetFlow - volume
        elif (volume >= 100):
            midDeal = midDeal + volume
            if (lineArr[5] == 'U'):
                midNetFlow = midNetFlow + volume
            if (lineArr[5] == 'D'):
                midNetFlow = midNetFlow - volume
        else:
            smallDeal = smallDeal + volume
            if (lineArr[5] == 'U'):
                smallNetFlow = smallNetFlow + volume
            if (lineArr[5] == 'D'):
                smallNetFlow = smallNetFlow - volume
        lastLineTimeStamp = currentLineTimeStamp
    result.finish = True
    result.computing = False
    print(result.gid + ' compute end\r\n')


def main_thread():
    while(len(gidArr)>0 or len(results) > 0):

        if (len(gidArr) > 0):
            i = 0
            for i in range(0, threadNum):
                if ((len(threads) == i or not threads[i].is_alive()) and len(gidArr) > 0):
                    try:
                        gid = gidArr.pop()
                    except:
                        continue
                    t = threading.Thread(target=get_tick, args=(gid,))
                    if (len(threads) == i):
                        threads.append(t)
                    else:
                        threads[i] = t
                    result = Result()
                    result.gid = gid
                    result.lines = []
                    results.append(result)
                    t.start()
                    #t.join()
                #t.join()

        #haveSthToCompute = False

        #i = 0
        #for i in range(0, len(results)):
        #    try:
        #        result = results[i]
        #        if (len(result.lines) == 0 or  result.finish or result.computing):
        #            continue
        #        haveSthToCompute = True
        #        t = threading.Thread(target=deal_result, args=(result,))

        #        t.start()
                #deal_result(result)
                #results.remove(result)
                #key = result.gid + '_money'
                #pipe.execute()
                #pipe.persist(key)
                #print(result.gid + ' saved')
        #    except Exception as err:
        #        print(str(err))
        #        break
        #if (not haveSthToCompute):
        #    i = 0
        #    for i in range(0, len(results)):
        #        results[i].try_times = results[i].try_times + 1
        #    time.sleep(1)
        print('---------------stop' + str(threadNum * 2) + '------------------------------')
        time.sleep(threadNum * 2)
        haveSthToSave = False
        i = 0
        for i in range(0, len(results)):
            try:
                result = results[i]
                if (not result.finish or result.saved):
                    continue
                haveSthToSave = True
                key = result.gid + '_money'
                j = 0
                for j in range(0, len(result.linesToSave)):
                    timeStr = result.linesToSave[j].split(',')[0]
                    timeStamp = util.get_timestamp(timeStr, '%Y-%m-%d %H:%M:%S')
                    pipe.zadd(key, {result.linesToSave[j]: timeStamp})

                pipe.execute()
                pipe.persist(key)
                print(result.gid + ' saved')
                result.saved = True
            except Exception as err:
                print(err)
                try:
                    result.saved = True
                except Exception as err:
                    print('remove err ' + str(err))
        if (not haveSthToSave):
            i = 0
            for i in range(0, len(results)):
                results[i].try_times = results[i].try_times + 1
            time.sleep(1)
        i = 0
        while(len(results) > 0 and i < len(results)):
            result = results[i]
            if (result.try_times > 2 or (result.finish and result.saved)):
                results.remove(result)
            else:
                i = i + 1

        print('----------------------------undeal ' + str(len(gidArr)) + ':'  + str(len(results)) + '---------------')
        try:
            redis.bgsave()
        except Exception as err:
            print(str(err))






            #for j in range(0, len(lines)):








def get_tick(gid):

    print(gid + ' start\r\n')

    if (gid[0:2] == 'kc'):
        newGid = gid[2:8] + '.SH'
    else:
        newGid = gid[2:8] + '.' + gid[0:2].upper()

    try:
        df = ts.realtime_tick(ts_code = newGid, src='dc')
        lines = []
        i = 0
        for i in range(0, df.index.size):
            type = 'E'
            if (df['TYPE'][i] == '买盘'):
                type = 'U'
            if (df['TYPE'][i] == '卖盘'):
                type = 'D'
            valueStr = df['TIME'][i] + ',' + str(df['PRICE'][i]) + ',' + str(df['CHANGE'][i]) + ',' + str(df['VOLUME'][i]) + ',' + str(df['AMOUNT'][i]) + ',' + type
            lines.append(valueStr)
        i = 0
        for i in range(0, len(results)):
            if (results[i].gid == gid):
                if (len(lines) > 0):
                    results[i].lines = lines
                    deal_result(results[i])
                else:
                    results.remove(results[i])
                break
    except Exception as err:
        print(err)
        i = 0
        for i in range(0, len(results)):
            if (results[i].gid == gid):
                results.remove(results[i])
                break
    print(gid + ' end\r\n')



threads = []
results = []
main_thread()