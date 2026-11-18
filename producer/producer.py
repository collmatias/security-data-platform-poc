"""
Mock security-event producer.

Generates synthetic email / web / DNS security telemetry (IPs, domains,
email addresses and a `threat_score`) and publishes it as JSON to a Kafka
(Redpanda) topic. A small pool of "known bad" indicators is reused so that
the same malicious IPs/domains show up repeatedly, like real campaigns do.

Examples:
    python producer.py --count 20000 --rate 500
    python producer.py --count 0 --rate 200          # run forever
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

EVENT_TYPES = ["email", "web", "dns"]
ACTIONS_CLEAN = ["delivered", "allowed"]
ACTIONS_BAD = ["quarantined", "blocked", "rewritten_url"]
THREAT_CATEGORIES = ["phishing", "malware", "spam", "c2", "credential_theft", "bec"]
LEGIT_DOMAINS = [
    "example.com", "contoso.com", "acme-corp.io", "globex.net", "initech.org",
    "umbrella-corp.com", "hooli.xyz", "piedpiper.com", "wayne-ent.com", "stark.io",
]
USERS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy"]

# Fixed pool of malicious indicators (simulates active campaigns).
rng = random.Random(42)
BAD_IPS = [f"185.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}" for _ in range(200)]
BAD_DOMAINS = [
    f"{rng.choice(['secure', 'login', 'verify', 'update', 'account'])}-"
    f"{rng.choice(['paypa1', 'micros0ft', 'app1e', 'amaz0n', 'g00gle', 'okta-sso'])}"
    f"{rng.randint(1, 999)}.{rng.choice(['ru', 'top', 'xyz', 'click', 'cn'])}"
    for _ in range(150)
]


def random_public_ip() -> str:
    return f"{random.randint(11, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def build_event(malicious_ratio: float) -> dict:
    is_bad = random.random() < malicious_ratio
    event_type = random.choice(EVENT_TYPES)
    if is_bad:
        source_ip = random.choice(BAD_IPS)
        domain = random.choice(BAD_DOMAINS)
        score = random.randint(81, 100)
        category = random.choice(THREAT_CATEGORIES)
        action = random.choice(ACTIONS_BAD)
    else:
        source_ip = random_public_ip()
        domain = random.choice(LEGIT_DOMAINS)
        # Mostly clean, with some "suspicious but below threshold" noise.
        score = random.choices([random.randint(0, 30), random.randint(31, 80)], weights=[85, 15])[0]
        category = "none" if score <= 30 else "suspicious"
        action = random.choice(ACTIONS_CLEAN)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "event_type": event_type,
        "source_ip": source_ip,
        "domain": domain,
        "sender": f"{random.choice(USERS)}@{domain}",
        "recipient": f"{random.choice(USERS)}@corp.local",
        "threat_score": score,
        "threat_category": category,
        "action": action,
    }


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    try:
        futures[topic].result()
        print(f"Created topic '{topic}' with {partitions} partitions")
    except KafkaException as exc:  # created concurrently by someone else
        print(f"Topic creation skipped: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Mock security event producer")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "security-events"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--count", type=int, default=10000, help="Events to send (0 = infinite)")
    parser.add_argument("--rate", type=float, default=500, help="Target events per second")
    parser.add_argument("--malicious-ratio", type=float, default=0.05)
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic, args.partitions)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,              # small batching window -> higher throughput
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
    })

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1

    sent, started = 0, time.monotonic()
    interval = 1.0 / args.rate if args.rate > 0 else 0
    print(f"Producing to {args.bootstrap}/{args.topic} at ~{args.rate}/s (count={args.count or 'inf'})")

    while not stop and (args.count == 0 or sent < args.count):
        event = build_event(args.malicious_ratio)
        try:
            # Key by source IP so all events from one IP land in the same partition.
            producer.produce(args.topic, key=event["source_ip"], value=json.dumps(event), on_delivery=on_delivery)
        except BufferError:
            producer.poll(0.5)
            continue
        sent += 1
        producer.poll(0)
        if sent % 5000 == 0:
            elapsed = time.monotonic() - started
            print(f"  sent={sent} ({sent / elapsed:.0f} ev/s) delivery_errors={errors}")
        if interval:
            # Pace against the wall clock so rate stays accurate.
            sleep_for = started + sent * interval - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    producer.flush(30)
    print(f"Finished: sent={sent} delivery_errors={errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
