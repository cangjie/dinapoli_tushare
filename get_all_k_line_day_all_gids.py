import sys
import datetime
import time
import tushare as ts
token = '4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec'
ts.set_token(token)
pro = ts.pro_api()
util = __import__('util')
kline_type = 'day'
if (len(sys.argv) >= 2):
    kline_type = sys.argv[1].strip()
redis = util.redis_client
pipe = util.redis_pipe
platform = util.get_current_os()

def deal_df(df):
    i = 0
    value_str = ''
    while(i < df.index.size):
        code = df['ts_code'][i]

        if (code[0:3] == '688'):
            ts_code = 'kc' + code[0:6]
        else:
            ts_code = code[7:9].lower() + code[0:6]

        trade_date = df['trade_date'][i]
        trade_date_str = trade_date[0:4] + '-' + trade_date[4:6] + '-' + trade_date[6:8]
        close = df['close'][i]
        open = df['open'][i]
        high = df['high'][i]
        low = df['low'][i]
        vol = int(df['vol'][i] * 100)
        str_key_name = ts_code + '_kline_day'
        timestamp = util.get_timestamp(trade_date, '%Y%m%d')
        value_str = ts_code + ',' + trade_date_str + ' 9:30:00,' + str(open) \
            + ',' + str(close) + ',' + str(high) + ',' + str(low) \
            + ',' + str(vol) + ',0,0'
        print(value_str)
        values = redis.zrangebyscore(str_key_name, timestamp, timestamp)

        if (len(values) == 0):
            pipe.zadd(str_key_name, {value_str: timestamp})
        i = i + 1
    pipe.persist(str_key_name)
    try:
        pipe.execute()
    except Exception as err:
        print(err)
        time.sleep(10)
        pipe.execute()
    finally:
        redis.bgsave()



def deal_code_list(codeList):
    getted = False
    while(not getted):
        try:
            df = pro.daily(ts_code=newCodeList, start_date=startDate, end_date=endDate)
            deal_df(df)
            getted = True
        except Exception as err:
            getted = False
            print(err)
            time.sleep(10)


    print('data getted:', df.index.size)



today_timestamp = str(int(datetime.datetime.now().timestamp()))
all_gids = redis.smembers('all_gids')
nowDate = datetime.datetime.now()
endDate = nowDate.strftime('%Y%m%d')
startDate = (nowDate + datetime.timedelta(days=-25)).strftime('%Y%m%d')
goNext = True
i = 0
newCodeList = ''
while (len(all_gids) > 0):
    if (goNext):
        code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    newCode = str_code[2:8] + '.' + str_code[0:2].replace('kc', 'sh').upper()
    if (newCodeList == '') :
        newCodeList = newCode
    else:
        newCodeList = newCodeList + ',' + newCode
    i = i + 1
    if (i%300==0):
        deal_code_list(newCodeList)
        time.sleep(5)
        i = 0
        newCodeList = ''
    print(str(len(all_gids)) + ' ' + newCode)
deal_code_list(newCodeList)




    #print(df)
