#!/usr/bin/env python3

import time

import uptrace
from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor

import redis

tracer = trace.get_tracer("app_or_package_name", "1.0.0")


def main():
    uptrace.configure_opentelemetry(
        dsn="http://project2_secret_token@localhost:14317/2",
        service_name="myservice",
        service_version="1.0.0",
    )
    RedisInstrumentor().instrument()

    client = redis.StrictRedis(host="localhost", port=6379)

    span = handle_request(client)
    print("trace:", uptrace.trace_url(span))

    for i in range(10000):
        handle_request(client)
        time.sleep(1)


def handle_request(client):
    with tracer.start_as_current_span(
        "handle-request", kind=trace.SpanKind.CLIENT
    ) as span:
        client.get("my-key")
        client.set("hello", "world")
        client.mset(
            {
                "employee_name": "Adam Adams",
                "employee_age": 30,
                "position": "Software Engineer",
            }
        )

        pipe = client.pipeline()
        pipe.set("foo", 5)
        pipe.set("bar", 18.5)
        pipe.set("blee", "hello world!")
        pipe.execute()

        return span


if __name__ == "__main__":
    main()
