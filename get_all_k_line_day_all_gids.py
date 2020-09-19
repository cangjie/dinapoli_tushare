import tushare as ts
import sys
util = __import__('util')
kline_type = 'day'
if (len(sys.argv) >= 2):
    kline_type = sys.argv[1].strip()


redis = util.redis_client
pipe = util.redis_pipe
platform = util.get_current_os()

df = ts.get_today_all()
i = df['code'].size-1
while (i>=0):
    code = df['code'][i]
    str_code = util.get_8_digit_gid(code)
    str_key_name = str_code + '_kline_' + kline_type.strip()
    kline_list = redis.zrange(str_key_name, 0, -1)
    df_kline = ts.get_hist_data(code)
    str_start_date = df_kline.index[df_kline.index.size - 1]
    if (len(kline_list) == 0):
        j = df_kline.index.size-1
        while(j>=0):
            str_kline_date = df_kline.index[j]
            timestamp = util.get_timestamp(str_kline_date, '%Y-%m-%d')
            value_str = str_kline_date
            if (platform == 'darwin' or platform == 'win32'):
                pipe.zadd(str_key_name, value_str, timestamp)
            else:
                if (platform == 'linux'):
                    pipe.zadd(str_key_name, {value_str: timestamp})
                else:
                    pipe.zadd(str_key_name, {value_str: timestamp})
            j = j - 1


    i = i - 1
