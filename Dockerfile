FROM fkrull/multi-python:latest

RUN apt update && apt install -y pypy pypy-dev pypy3-dev curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py
RUN pip install codecov

COPY . /redis-py
