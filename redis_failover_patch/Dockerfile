FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN python -m pip install --upgrade pip && \
    python -m pip install -e . && \
    python -m pip install pytest mock

CMD ["/bin/bash"]
