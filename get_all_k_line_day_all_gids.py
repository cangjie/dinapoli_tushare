import tushare as ts
import sys
import datetime
util = __import__('util')
kline_type = 'day'
if (len(sys.argv) >= 2):
    kline_type = sys.argv[1].strip()
redis = util.redis_client
pipe = util.redis_pipe
platform = util.get_current_os()
today_timestamp = str(int(datetime.datetime.now().timestamp()))
all_gids = redis.smembers('all_gids')
while (len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)
    str_key_name = str_code + '_kline_' + kline_type.strip()
    kline_list = redis.zrange(str_key_name, 0, -1)
    df_kline = ts.get_hist_data(str_code[2:8])
    if (df_kline is None):
        continue
    str_start_date = df_kline.index[df_kline.index.size - 1]
    timestamp_start_date = util.get_timestamp(str_start_date, '%Y-%m-%d')
    pipe.zremrangebyscore(str_key_name, timestamp_start_date, today_timestamp)
    j = df_kline.index.size-1
    while(j>=0):
            str_kline_date = df_kline.index[j]
            timestamp = util.get_timestamp(str_kline_date, '%Y-%m-%d')
            value_str = str_code + ',' + str_kline_date + ' 9:30:00,' + str(df_kline['open'][j]) \
                + ',' + str(df_kline['close'][j]) + ',' + str(df_kline['high'][j]) + ',' + str(df_kline['low'][j]) \
                + ',' + str(int(100 * float(df_kline['volume'][j]))) + ',0'

            pipe.zadd(str_key_name, {value_str: timestamp})
            #if (platform == 'darwin' or platform == 'win32'):
            #    pipe.zadd(str_key_name, value_str, timestamp)
            #else:
            #    if (platform == 'linux'):
            #        pipe.zadd(str_key_name, {value_str: timestamp})
            #    else:
            #        pipe.zadd(str_key_name, {value_str: timestamp})
            pipe.persist(str_key_name)
            j = j - 1
    pipe.execute()
redis.bgsave()