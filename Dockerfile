FROM fkrull/multi-python:latest

RUN apt update && apt install -y pypy pypy-dev pypy3-dev

WORKDIR /redis-py

COPY . /redis-py
