"""
Redis Pub/Sub Pattern
======================

Demonstrates the publish/subscribe messaging pattern with redis-py.

This script runs both a subscriber and a publisher in the same process
using threads so you can see the full flow without opening two terminals.

Commands covered:
  - SUBSCRIBE / PSUBSCRIBE (pattern-based)
  - PUBLISH
  - Unsubscribing and cleaning up

Prerequisites:
  pip install redis

  A running Redis server (default: localhost:6379).
  Start one with Docker:
    docker run -d -p 6379:6379 redis
"""

import threading
import time

import redis


def subscriber(ready_event: threading.Event) -> None:
    """Listen for messages on the 'notifications' channel."""
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = r.pubsub()

    # Subscribe to a specific channel
    pubsub.subscribe("notifications")

    # Subscribe to a pattern -- receives messages from any channel
    # whose name starts with "events."
    pubsub.psubscribe("events.*")

    print("[subscriber] Subscribed to 'notifications' and 'events.*'")

    # Signal the main thread that we are ready to receive messages.
    ready_event.set()

    # listen() yields messages as they arrive.  The first messages are
    # subscription confirmations (type "subscribe" / "psubscribe").
    for message in pubsub.listen():
        msg_type = message["type"]

        if msg_type in ("subscribe", "psubscribe"):
            # Confirmation of subscription -- nothing to process.
            continue

        if msg_type in ("message", "pmatch"):
            channel = message["channel"]
            data = message["data"]
            print(f"[subscriber] Received on '{channel}': {data}")

            # Use a sentinel value to know when to stop.
            if data == "QUIT":
                break

    pubsub.unsubscribe()
    pubsub.punsubscribe()
    pubsub.close()
    print("[subscriber] Unsubscribed and closed.")


def publisher(ready_event: threading.Event) -> None:
    """Publish a few messages after the subscriber is ready."""
    # Wait until the subscriber thread has set up its subscriptions.
    ready_event.wait()
    # Small extra delay to ensure subscription is fully registered.
    time.sleep(0.1)

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Publish to the exact channel
    count = r.publish("notifications", "Hello from publisher!")
    print(f"[publisher]  Published to 'notifications' ({count} receivers)")

    # Publish to a pattern-matched channel
    count = r.publish("events.login", "user:42 logged in")
    print(f"[publisher]  Published to 'events.login' ({count} receivers)")

    count = r.publish("events.purchase", "order:99 placed")
    print(f"[publisher]  Published to 'events.purchase' ({count} receivers)")

    # Send the sentinel so the subscriber exits cleanly.
    r.publish("notifications", "QUIT")


def main() -> None:
    ready = threading.Event()

    sub_thread = threading.Thread(target=subscriber, args=(ready,))
    pub_thread = threading.Thread(target=publisher, args=(ready,))

    sub_thread.start()
    pub_thread.start()

    pub_thread.join()
    sub_thread.join()

    print("Done.")


if __name__ == "__main__":
    main()
