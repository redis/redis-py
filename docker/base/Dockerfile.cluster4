# produces redisfab/redis-py-cluster:4.0
FROM redis:4.0-buster

COPY create_cluster4.sh /create_cluster4.sh
RUN chmod +x /create_cluster4.sh

EXPOSE 16391 16392 16393 16394 16395 16396

CMD [ "/create_cluster4.sh"]