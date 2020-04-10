import os
config = __import__('config')
import numpy
import pandas
import tushare
import redis
import time
from pathlib import Path
import pyodbc

data_path = os.getcwd()
path_splitor = '/'
if (data_path.find('\\') >= 0):
    path_splitor = '\\'
data_path = data_path + path_splitor + config.data_sub_path
redis_client = redis.Redis(host=config.redis_ip, port=config.redis_port, db=config.redis_db)
redis_pipe = redis_client.pipeline()
path_obj = Path(data_path)
if (not(path_obj.is_dir())):
    path_obj.mkdir('data')

def get_sql_server_connect_string():
    return 'DRIVER=' + config.sql_server_driver + ';SERVER=52.80.17.211,9753;DATABASE=stock;UID=sa;PWD=Jarrod780209'

def get_sql_server_conn():
    return pyodbc.connect(get_sql_server_connect_string())

def refresh_file(path, encoding='utf-8'):
    file_obj = Path(path)
    if (file_obj.is_file()):
        os.remove(path)
    f = open(path, 'ab')
    return f

def persist_all_gids():
    all_gids_arr = redis_client.smembers('all_gids')
    file_name = data_path+path_splitor+'all_gids_list.txt'
    f = refresh_file(file_name)
    for gid_name_pair in all_gids_arr:
        gid_name_pair_str = gid_name_pair.decode(encoding='utf-8')
        gid = gid_name_pair_str.split(' ')[0]
        name = gid_name_pair_str.split(' ')[1]
        f.write((gid+'\t'+name+'\r\n').encode('utf-8'))
    f.close()

def get_8_digit_gid(gid):
    if (gid[0:2] == '68'):
        gid = 'kc' + gid
    elif (gid[0:1] == '6'):
        gid = 'sh' + gid
    else:
        gid = 'sz' + gid
    return gid

def refresh_all_gids():
    df = tushare.get_today_all()
    s_gid = df['code']
    s_name = df['name']
    i = 0
    redis_pipe.delete('all_gids')
    while i < s_gid.size:
        gid = s_gid[i]
        if (gid[0:2] == '68'):
            gid = 'kc' + gid
        elif (gid[0:1] == '6'):
            gid = 'sh' + gid
        else:
            gid = 'sz' + gid
        redis_pipe.sadd('all_gids', gid + ' ' + s_name[i])
        i = i + 1
    redis_pipe.expire('all_gids', 31536000)
    redis_pipe.bgsave()
    redis_pipe.execute()

def get_timestamp(date_time, date_time_format):
    time_arr = time.strptime(date_time, date_time_format)
    return int(time.mktime(time_arr))

def get_single_gid_df_from_tushare(gid, ktype):
    gid = str(gid)
    if (gid.__len__()>6):
        gid = gid[gid.__len__()-6, 6]
    df = tushare.get_hist_data(gid, ktype=ktype)
    save_df_to_redis(gid, ktype, df)
    return df

def save_df_to_redis(gid, ktype, df):
    redis_key_name = gid+'_'+ktype
    redis_pipe.delete(redis_key_name)
    i = 0
    date_time_format = '%Y-%m-%d'
    while (i < df.index.size):
        date_time = str(df.index[i])
        open = str(df['open'][i])
        close = str(df['close'][i])
        high = str(df['high'][i])
        low = str(df['low'][i])
        volume = str(df['volume'][i])
        timestamp = get_timestamp(date_time, date_time_format)
        redis_pipe.zadd(redis_key_name, date_time+','+open+','+close+','+high+','+low+','+volume, float(timestamp))
        i = i + 1
    redis_pipe.expire(redis_key_name, 31536000)
    redis_pipe.execute()
    redis_pipe.bgsave()

def load_df_from_redis(gid, ktype):
    df = pandas.DataFrame()
    redis_key_name = gid + '_' + ktype
    redis_set = redis_client.zrange(redis_key_name, 0, -1)
    i = 0
    date_time_arr = []
    open_arr = []
    close_arr = []
    high_arr = []
    low_arr = []
    volume_arr = []
    #date_time_arr = numpy.empty([redis_set.__len__()], dtype = str)
    #open_arr = numpy.empty([redis_set.__len__()], dtype = str)
    #close_arr = numpy.empty([redis_set.__len__()], dtype = str)
    #high_arr = numpy.empty([redis_set.__len__()], dtype = str)
    #low_arr = numpy.empty([redis_set.__len__()], dtype = str)
    #volume_arr = numpy.empty([redis_set.__len__()], dtype = str)
    while i < redis_set.__len__():
        line_arr = str(redis_set[i]).split(',')
        date_time_arr.append(line_arr[0].replace("b'", ""))
        open_arr.append(line_arr[1])
        close_arr.append(line_arr[2])
        high_arr.append(line_arr[3])
        low_arr.append(line_arr[4])
        volume_arr.append(line_arr[5].replace("'", ""))
        i = i + 1
    df['open'] = pandas.Series(open_arr, index=date_time_arr)
    df['close'] = pandas.Series(close_arr, index=date_time_arr)
    df['high'] = pandas.Series(high_arr, index=date_time_arr)
    df['low'] = pandas.Series(low_arr, index=date_time_arr)
    df['volume'] = pandas.Series(volume_arr, index=date_time_arr)
    return df

#print(load_df_from_redis('600031', 'D'))
#print(get_timestamp('2020-04-01', '%Y-%m-%d'))


#get_single_gid_df_from_tushare('600031', 'D')

#refresh_all_gids()
#persist_all_gids()

#df = tushare.get_hist_data('600031')
#date_arr = df.T
#open_arr = df['open']
#high_arr = df['high']
#close_arr = df['close']
#low_arr = df['low']
#volume_arr = df['volume']
#turnover_arr = df['turnover']
#print(df)