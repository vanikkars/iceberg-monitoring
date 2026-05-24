"""
Synthetic event producer — batch mode.

Publishes JSON events to two Kafka topics:
  - users        : user registration events (seeded once up-front)
  - transactions : financial transactions referencing valid user IDs

Batch behaviour
---------------
Sends TOTAL_ROWS transactions split evenly across N_BATCHES batches,
one batch per BATCH_INTERVAL_SECONDS.  After the final batch the
process exits cleanly.

  default: 1 000 000 rows, 10 batches, 60 s interval
           → 100 000 rows/min for 10 minutes

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
import logging
import os
import random
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WAIT_TILL_BOOT       = int(os.environ.get("WAIT_TILL_BOOT", "240"))
TOTAL_ROWS           = int(os.environ.get("TOTAL_ROWS", "1000000"))
N_BATCHES            = int(os.environ.get("N_BATCHES", "10"))
BATCH_INTERVAL_S     = int(os.environ.get("BATCH_INTERVAL_SECONDS", "60"))
N_USERS              = 50

TXN_TYPES    = ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT"]
TXN_STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
CURRENCIES   = ["USD", "EUR", "GBP", "CAD", "AUD"]
COUNTRIES    = ["US", "DE", "GB", "CA", "AU", "FR", "JP", "BR"]

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
        transaction_id=f"txn-{seq:07d}",
        user_id=user.user_id,
        amount=round(random.uniform(1.0, 2000.0), 2),
        currency=random.choice(CURRENCIES),
        type=random.choice(TXN_TYPES),
        status=random.choice(TXN_STATUSES),
        event_time=_now(),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    rows_per_batch = TOTAL_ROWS // N_BATCHES

    log.info("Connecting to Kafka at %s …", BOOTSTRAP_SERVERS)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=5,
        batch_size=65536,
    )

    log.info("Waiting %s s for the environment to set up …", WAIT_TILL_BOOT)
    time.sleep(WAIT_TILL_BOOT)

    # 1. Seed users
    log.info("Seeding %d users into 'users' topic …", N_USERS)
    user_pool = _build_user_pool(N_USERS)
    for user in user_pool:
        producer.send("users", value=asdict(user))
    producer.flush()
    log.info("User seed complete.")

    # 2. Batch loop
    log.info(
        "Starting batch run: %d batches × %d rows, interval %ds  (total: %d rows)",
        N_BATCHES, rows_per_batch, BATCH_INTERVAL_S, N_BATCHES * rows_per_batch,
    )

    txn_seq = 1
    for batch in range(1, N_BATCHES + 1):
        batch_start = time.monotonic()
        log.info("Batch %d/%d — sending %d rows …", batch, N_BATCHES, rows_per_batch)

        for _ in range(rows_per_batch):
            txn = make_transaction(txn_seq, user_pool)
            producer.send("transactions", value=asdict(txn))
            txn_seq += 1

        producer.flush()
        sent_so_far = (batch) * rows_per_batch
        log.info("Batch %d/%d done — %d rows sent so far", batch, N_BATCHES, sent_so_far)

        if batch < N_BATCHES:
            elapsed  = time.monotonic() - batch_start
            sleep_for = max(0.0, BATCH_INTERVAL_S - elapsed)
            log.info("Sleeping %.1f s until next batch …", sleep_for)
            time.sleep(sleep_for)

    log.info("All %d rows published. Producer exiting.", N_BATCHES * rows_per_batch)


if __name__ == "__main__":
    main()