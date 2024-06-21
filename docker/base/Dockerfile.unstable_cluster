# produces redisfab/redis-py-cluster:6.2.6
FROM redisfab/redis-py:unstable-bionic

COPY create_cluster.sh /create_cluster.sh
RUN chmod +x /create_cluster.sh

EXPOSE 6372 6373 6374 6375 6376 6377

ENV START_PORT=6372
ENV END_PORT=6377
CMD ["/create_cluster.sh"]
