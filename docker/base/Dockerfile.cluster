# produces redisfab/redis-py-cluster:6.2.6
FROM redis:6.2.6-buster

COPY create_cluster.sh /create_cluster.sh
RUN chmod +x /create_cluster.sh

EXPOSE 16379 16380 16381 16382 16383 16384

ENV START_PORT=16379
ENV END_PORT=16384
CMD /create_cluster.sh
