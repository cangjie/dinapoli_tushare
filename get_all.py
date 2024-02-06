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
today_timestamp = str(int(datetime.datetime.now().timestamp()))
all_gids = redis.smembers('all_gids')
nowDate = datetime.datetime.now()
startDate = nowDate.strftime('%Y%m%d')
endDate = (nowDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')
goNext = True
while (len(all_gids) > 0):
    if (goNext):
        code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    newCode = str_code[2:8] + '.' + str_code[0:2].replace('kc', 'sh').upper()
    print(str(len(all_gids)) + ' ' + newCode)
    try:
        df = ts.pro_bar(ts_code=newCode, asset='E', adj='qfq', start_date=startDate, end_date=endDate)
        goNext = True
    except:
        time.sleep(10)
        goNext = False

    #print(df)
