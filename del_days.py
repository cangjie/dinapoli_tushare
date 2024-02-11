import sys
import datetime
day = sys.argv[1]
currentDate = datetime.datetime.strptime(day, '%Y%m%d')
util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe
all_gids = redis.smembers('all_gids')
while(len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    key = str_code + '_kline_day'
    values = redis.zrange(key, 0, -1)
    while(len(values) > 0):
        value = values.pop()
        valueArr = str(value).split(',')
        if (len(valueArr) > 1):
            dayStr = valueArr[1].split(' ')[0]
            redisDate = datetime.datetime.strptime(dayStr, '%Y-%m-%d')
            if (redisDate == currentDate):
                redis.zrem(key, value)
                print(str(value))
                pipe.persist(key)
redis.bgsave()
