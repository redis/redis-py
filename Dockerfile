FROM ubuntu:20.04

COPY build_tools /build_tools

RUN /build_tools/bootstrap.sh
RUN /build_tools/build_redis.sh
RUN /build_tools/install_redis.sh
RUN /build_tools/install_sentinel.sh

COPY build_tools/start.sh /var/lib/redis/start.sh

EXPOSE 6379
EXPOSE 6380
EXPOSE 26379
EXPOSE 26380
EXPOSE 26381

CMD /var/lib/redis/start.sh
