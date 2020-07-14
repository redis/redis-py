FROM fkrull/multi-python:latest

RUN apt install -y pypy pypy-dev pypy3-dev
COPY . /redis-py
