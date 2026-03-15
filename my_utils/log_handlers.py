import logging

# class RedisHandler(logging.Handler):
#     def __init__(self, channel, host='localhost', port=6379, db=0):
#         super().__init__()
#         self.redis_client = redis.Redis(host=host, port=port, db=db)
#         self.channel = channel

#     def emit(self, record):
#         log_entry = self.format(record)
#         self.redis_client.publish(self.channel, log_entry)