FROM redis:6.0.6-buster

RUN apt update && apt install openssl

COPY gen-test-certs.sh /tests/gen-test-certs.sh

RUN /tests/gen-test-certs.sh && chown -R redis:redis /tests