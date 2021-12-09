#!/bin/bash

DESTDIR=`dirname "$0"`/keys
if [ ! -d ${DESTDIR} ]; then
    mkdir `dirname "$0"`/keys
fi
cd ${DESTDIR}

SSL_SUBJECT="/C=CA/ST=Winnipeg/L=Manitoba/O=Some Corp/OU=IT Department/CN=example.com"
which openssl &>/dev/null
if [ $? -ne 0 ]; then
	echo "No openssl binary present, exiting."
	exit 1
fi

openssl genrsa -out ca-key.pem 2048

openssl req -new -x509 -nodes -days 365000 \
   -key ca-key.pem \
   -out ca-cert.pem \
   -subj "${SSL_SUBJECT}"

openssl req -newkey rsa:2048 -nodes -days 365000 \
   -keyout server-key.pem \
   -out server-req.pem \
   -subj "${SSL_SUBJECT}"

openssl x509 -req -days 365000 -set_serial 01 \
   -in server-req.pem \
   -out server-cert.pem \
   -CA ca-cert.pem \
   -CAkey ca-key.pem

openssl req -newkey rsa:2048 -nodes -days 365000 \
   -keyout client-key.pem \
   -out client-req.pem \
   -subj "${SSL_SUBJECT}"

openssl x509 -req -days 365000 -set_serial 01 \
   -in client-req.pem \
   -out client-cert.pem \
   -CA ca-cert.pem \
   -CAkey ca-key.pem
