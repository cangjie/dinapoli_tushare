import os
config = __import__('config')
import numpy
import pandas
import tushare
import redis
from pathlib import Path


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

#refresh_all_gids()
persist_all_gids()