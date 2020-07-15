FROM redis-py-base:latest

COPY redis.conf /usr/local/etc/redis/redis.conf

EXPOSE 26380

CMD [ "redis-sentinel", "/usr/local/etc/redis/redis.conf" ]
