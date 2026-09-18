"""Send synthetic ad events into Kafka."""

import argparse
import json
import time

from kafka import KafkaProducer

from events import make_event

TOPIC = "ad-events"

def build_producer(servers="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

def main(count, rate):
    producer = build_producer()
    sent = 0
    try:
        while count == 0 or sent < count:
            event = make_event()
            producer.send(TOPIC, key=event["campaign_id"], value=event)
            sent += 1
            if sent % 100 == 0:
                print(f"sent {sent} events")
            time.sleep(1 / rate)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        producer.flush()
        producer.close()
        print(f"done, {sent} events sent")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=500, help="0 means run forever")
    parser.add_argument("--rate", type=float, default=20, help="events per second")
    main(*vars(parser.parse_args()).values())