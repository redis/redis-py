# produces redisfab/stunnel:latest
FROM ubuntu:18.04

RUN apt-get update -qq --fix-missing
RUN apt-get upgrade -qqy
RUN apt install -qqy stunnel
RUN mkdir -p /etc/stunnel/conf.d
RUN echo "foreground = yes\ninclude = /etc/stunnel/conf.d" > /etc/stunnel/stunnel.conf
RUN chown -R root:root /etc/stunnel/

CMD ["/usr/bin/stunnel"]
