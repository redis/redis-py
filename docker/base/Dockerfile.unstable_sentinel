# produces redisfab/redis-py-sentinel:unstable
FROM ubuntu:bionic as builder
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y build-essential git
RUN mkdir /build
WORKDIR /build
RUN git clone https://github.com/redis/redis
WORKDIR /build/redis
RUN make

FROM ubuntu:bionic as runner
COPY --from=builder /build/redis/src/redis-server /usr/bin/redis-server
COPY --from=builder /build/redis/src/redis-cli /usr/bin/redis-cli
COPY --from=builder /build/redis/src/redis-sentinel /usr/bin/redis-sentinel

CMD ["redis-sentinel", "/sentinel.conf"]
