FROM redis-py-base:latest

COPY sentinel.conf /usr/local/etc/redis/sentinel.conf

EXPOSE 26380

CMD [ "redis-sentinel", "/usr/local/etc/redis/sentinel.conf" ]
