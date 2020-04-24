import tushare
import time
util = __import__('util')
config = __import__('config')
util.append_log('get_realtime_quotes.log', 'start snap.')
all_gids_arr = util.redis_client.smembers('all_gids')
pipe = util.redis_pipe
redis = util.redis_client
i = 0
gid_arr = []
for gid_name_pair in all_gids_arr:
    gid_name_pair_str = gid_name_pair.decode(encoding='utf-8')
    gid_arr.append(gid_name_pair_str.split(' ')[0])
    i = i + 1


runtimes = config.get_all_realtime_data_run_times_per_minute

while (runtimes > 0):
    runtimes = runtimes - 1
    step = 30
    start_index = 0
    end_index = start_index + step - 1
    while start_index < gid_arr.__len__() :
        gid_sub_arr = []
        i = start_index
        while (i <= end_index):
            gid_sub_arr.append(gid_arr[i][2:8])
            i = i + 1
        try:
            df = tushare.get_realtime_quotes(gid_sub_arr)
            print(df)
            j = 0
            while (j < df['code'].size):
                date_time_str = str(df['date'][j]) + ' ' + str(df['time'][j])
                timestamp = util.get_timestamp(date_time_str, '%Y-%m-%d %H:%M:%S')
                key_str = str(df['date'][j])[0:4] + str(df['date'][j])[5:7] + str(df['date'][j])[8:10]
                key_str = str(df['code'][j]) + '_' + key_str + '_quotes'
                value_str = date_time_str + ',' + str(df['open'][j]) + ',' + str(df['price'][j]) + ',' + str(df['high'][j]) \
                            + ',' + str(df['low'][j] + ',' + str(df['volume'][j]))
                last_line_set = redis.zrange(key_str, 0, -1, withscores=1)
                exists = False
                if (len(last_line_set)>0):
                    last_line = last_line_set[len(last_line_set)- 1]
                    if (int(timestamp) <= int(last_line[1])):
                        exists = True
                if (not(exists)):
                    try:
                        platform = util.get_current_os()
                        if (platform == 'darwin'):
                            pipe.zadd(key_str, value_str, timestamp)
                        else:
                            if (platform == 'linux'):
                                pipe.zadd(key_str, {value_str: timestamp})
                            else:
                                pipe.zadd(key_str, {value_str: timestamp})
                    except Exception as redis_err:
                        print(str(redis_err) + '\r\n')
                    pipe.expire(key_str, 0)
                j = j + 1
        except Exception as e:
            print(str(e) + '\r\n')
        finally:
            pipe.execute()
        start_index = end_index + 1
        end_index = start_index + step - 1
        if (end_index >= gid_arr.__len__()):
            end_index = gid_arr.__len__() - 1
    time.sleep(10)
try:
    pipe.bgsave()
except:
    print('bgsave error')
util.append_log('get_realtime_quotes.log', 'finish snap.')


