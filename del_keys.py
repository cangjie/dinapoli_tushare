util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe
all_gids = redis.smembers('all_gids')
while (len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)
    str_key_name = str_code + '_kline_week'
    redis.delete(str_key_name)
redis.bgsave()
