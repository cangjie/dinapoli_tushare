util = __import__('util')
pipe = util.redis_pipe
pipe.zadd('test_zadd', { 'aaaa': 100})
pipe.execute()