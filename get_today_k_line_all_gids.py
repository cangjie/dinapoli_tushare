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
str_current_date = str(datetime.datetime.now())
str_current_date = str_current_date.split(' ')[0]
if (util.is_holiday(str_current_date)):
    exit()
df = ts.get_today_all()
i = 0
while (i < df['code'].size):
    gid = df['code'][i]
    gid = util.get_8_digit_gid(gid)
    print(str(df['code'].size - i) + ' ' + gid)
    str_key_name = gid + '_kline_' + kline_type
    kline_list = redis.zrange(str_key_name, 0, -1)
    last_index = len(kline_list)-1
    if (last_index < 0):
        i = i + 1
        continue
    str_last_date = str(kline_list[last_index]).split(',')[1].strip()
    str_last_date = str_last_date.split(' ')[0]
    if (str_current_date == str_last_date):
        pipe.zremrangebyrank(str_key_name, last_index, last_index)

    value_str = gid+','+str_current_date+' 09:30:00,'+str(df['open'][i])+','+str(df['trade'][i]) + ','\
        + str(df['high'][i]) + ',' + str(df['low'][i]) + ',' + str(int(df['volume'][i])) + ',' \
        + str(df['amount'][i])
    timestamp = util.get_timestamp(str_current_date, '%Y-%m-%d')
    #print(timestamp)
    if (platform == 'darwin' or platform == 'win32'):
        pipe.zadd(str_key_name, value_str, timestamp)
        print('zadd ' + str(timestamp) + ' ' + value_str + ' ' + platform)
    else:
        if (platform == 'linux'):
            pipe.zadd(str_key_name, {value_str: timestamp})
            print('zadd ' + str(timestamp) + ' ' + value_str + ' ' + platform)
        else:
            pipe.zadd(str_key_name, {value_str: timestamp})
            print('zadd ' + str(timestamp) + ' ' + value_str + ' ' + platform)
    pipe.persist(str_key_name)
    pipe.execute()
    value_str = ''
    i = i + 1
