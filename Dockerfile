FROM fkrull/multi-python:latest

RUN apt update \
    && apt install -y pypy pypy-dev pypy3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /redis-py

COPY . /redis-py
