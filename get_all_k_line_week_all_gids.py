import tushare as ts
import sys
import datetime
import time
util = __import__('util')
kline_type = 'week'
if (len(sys.argv) >= 2):
    kline_type = sys.argv[1].strip()
redis = util.redis_client
pipe = util.redis_pipe
platform = util.get_current_os()
today_timestamp = str(int(datetime.datetime.now().timestamp()))
all_gids = redis.smembers('all_gids')

while (len(all_gids) > 0):
    i = len(all_gids)
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)
    str_key_name = str_code + '_kline_' + kline_type.strip()

    #redis.delete(str_key_name)
    #continue

    df_kline = ts.get_hist_data(str_code[2:8], ktype='W')
    #df_kline = ts.get_hist_data('sz833971', ktype='W')
    if (df_kline is None):
        continue

    j = df_kline.index.size-1

    while (j>=0):
        str_kline_date = df_kline.index[j]
        timestamp = util.get_timestamp(str_kline_date, '%Y-%m-%d')
        value_str = str_code + ',' + str_kline_date + ' 15:00:00,' + str(df_kline['open'][j]) \
            + ',' + str(df_kline['close'][j]) + ',' + str(df_kline['high'][j]) + ',' + str(df_kline['low'][j]) \
            + ',' + str(int(100 * float(df_kline['volume'][j]))) + ',0'
        pipe.zadd(str_key_name, {value_str: timestamp})
        j = j - 1

    pipe.persist(str_key_name)
    try:
        pipe.execute()
        #time.sleep(0.2)
    except Exception as err:
        print(err)
        time.sleep(30)
        print('try to resave')
        pipe.execute()
        #time.sleep(0.2)
        print('resave success')


try:

    pipe.execute()
    time.sleep(15)
except:
    time.sleep(60)
    pipe.execute()
    time.sleep(15)
redis.bgsave()