import tushare
import time
util = __import__('util')

label_change = 0.02

date_str = time.strftime('%Y-%m-%d', time.localtime(time.time()))
all_gids_arr = util.redis_client.smembers('all_gids')
for gid_name_pair in all_gids_arr:
    gid_name_pair_str = gid_name_pair.decode(encoding='utf-8')
    gid = gid_name_pair_str.split(' ')[0]
    gid = gid[2:8]
    print('Get ticks:' + gid+'\r\n')
    df = tushare.get_today_ticks(gid)
    print('\r\n')
    i = 0
    low_price = 999999999
    low_index = i
    while (i < df['price'].size):
        if (low_price > float(df['price'][i])):
            low_price = float(df['price'][i])
            low_index = i
        i = i + 1
    print(gid + ' ' + str(df['time'][low_index]) + ' ' + str(low_price))
    time.sleep(10)