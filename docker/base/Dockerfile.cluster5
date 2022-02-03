# produces redisfab/redis-py-cluster:5.0
FROM redis:5.0-buster

COPY create_cluster5.sh /create_cluster5.sh
RUN chmod +x /create_cluster5.sh

EXPOSE 16385 16386 16387 16388 16389 16390

CMD [ "/create_cluster5.sh"]