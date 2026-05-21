"""
Synthetic event producer.

Publishes JSON events to two Kafka topics:
  - users        : user registration events
  - transactions : financial transactions referencing valid user IDs

Users are seeded up-front so that transactions always reference an
existing user_id — referential integrity is guaranteed within the
producer.

Event shapes
------------
users:
  { "user_id": "user-001", "name": "Alice Smith", "email": "alice@example.com",
    "country": "US", "created_at": "2024-04-22T10:00:00.000" }

transactions:
  { "transaction_id": "txn-000001", "user_id": "user-001", "amount": 49.99,
    "currency": "USD", "type": "PURCHASE", "status": "COMPLETED",
    "event_time": "2024-04-22T10:00:00.000" }
"""

import json
import os
import random
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "2"))
WAIT_TILL_BOOT = int(os.environ.get('WAIT_TILL_BOOT', 120)) # wait for 2 minutes before producing messages

N_USERS = 50

TXN_TYPES          = ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT"]
TXN_STATUSES       = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
CURRENCIES         = ["USD", "EUR", "GBP", "CAD", "AUD"]
COUNTRIES          = ["US", "DE", "GB", "CA", "AU", "FR", "JP", "BR"]

FIRST_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace",
               "Hank", "Iris", "Jack", "Karen", "Leo", "Mia", "Ned",
               "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tara"]
LAST_NAMES  = ["Smith", "Jones", "Williams", "Brown", "Taylor", "Wilson",
               "Davies", "Evans", "Thomas", "Roberts", "Johnson", "White",
               "Martin", "Garcia", "Martinez", "Robinson", "Clark", "Lewis"]


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class UserEvent:
    user_id:    str
    name:       str
    email:      str
    country:    str
    created_at: str


@dataclass
class TransactionEvent:
    transaction_id: str
    user_id:        str
    amount:         float
    currency:       str
    type:           str
    status:         str
    event_time:     str


# ── Factory helpers ───────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _build_user_pool(n: int) -> list[UserEvent]:
    """Pre-generate the full set of users so downstream events can reference them."""
    users = []
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        users.append(UserEvent(
            user_id=f"user-{i:03d}",
            name=f"{first} {last}",
            email=f"{first.lower()}.{last.lower()}{i}@example.com",
            country=random.choice(COUNTRIES),
            created_at=_now(),
        ))
    return users


def make_transaction(seq: int, user_pool: list[UserEvent]) -> TransactionEvent:
    user = random.choice(user_pool)
    return TransactionEvent(
        transaction_id=f"txn-{seq:06d}",
        user_id=user.user_id,
        amount=round(random.uniform(1.0, 2000.0), 2),
        currency=random.choice(CURRENCIES),
        type=random.choice(TXN_TYPES),
        status=random.choice(TXN_STATUSES),
        event_time=_now(),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} …", flush=True)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f'waiting for the environment to setup: {WAIT_TILL_BOOT}')
    time.sleep(WAIT_TILL_BOOT)


    # 1. Seed users — publish all of them before any transactions/orders
    print(f"Seeding {N_USERS} users into 'users' topic …", flush=True)
    user_pool = _build_user_pool(N_USERS)
    for user in user_pool:
        producer.send("users", value=asdict(user))
        print(f"[users] → {asdict(user)}", flush=True)
    producer.flush()
    print("User seed complete.", flush=True)

    # 2. Continuous stream of transactions
    print(f"Publishing transactions at {EVENTS_PER_SECOND} events/sec …", flush=True)
    interval = 1.0 / EVENTS_PER_SECOND
    txn_seq  = 1

    while True:
        txn = make_transaction(txn_seq, user_pool)
        producer.send("transactions", value=asdict(txn))
        print(f"[transactions] → {asdict(txn)}", flush=True)
        txn_seq += 1
        time.sleep(interval)


if __name__ == "__main__":
    main()