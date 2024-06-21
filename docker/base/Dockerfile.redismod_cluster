# produces redisfab/redis-py-modcluster:6.2.6
FROM redislabs/redismod:edge

COPY create_redismod_cluster.sh /create_redismod_cluster.sh
RUN chmod +x /create_redismod_cluster.sh

EXPOSE 46379 46380 46381 46382 46383 46384

ENV START_PORT=46379
ENV END_PORT=46384
ENTRYPOINT []
CMD /create_redismod_cluster.sh
