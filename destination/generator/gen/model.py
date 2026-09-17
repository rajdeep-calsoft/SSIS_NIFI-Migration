"""Synthetic e-commerce order generation.

The pipeline consumes FLAT order-line records: one JSON object per line item,
with the order-level fields repeated on every line. That is what a real OMS
flat-file export looks like, and it lets NiFi do the interesting work --
a genuine GROUP BY aggregation in QueryRecord to rebuild order headers --
instead of wrestling nested arrays through a Jolt spec.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# NiFi readers are configured with: yyyy-MM-dd'T'HH:mm:ss'Z'
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

STATUSES = ["PLACED", "PAID", "PAID", "PAID", "SHIPPED", "SHIPPED", "CANCELLED"]
CHANNELS = ["WEB", "WEB", "WEB", "MOBILE", "MOBILE", "STORE", "PARTNER"]
PAYMENTS = ["UPI", "UPI", "CARD", "CARD", "NETBANKING", "COD", "WALLET"]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


@dataclass
class Product:
    sku: str
    unit_price: float
    category: str


class Catalog:
    """Products and customers, read from the seeded dimension tables so the
    generated facts are referentially consistent with the database."""

    def __init__(self, products: list[Product], customer_ids: list[str]):
        if not products or not customer_ids:
            raise RuntimeError("catalog is empty - are the dimension tables seeded?")
        self.products = products
        self.customer_ids = customer_ids

    @classmethod
    def from_db(cls, conn) -> "Catalog":
        with conn.cursor() as cur:
            cur.execute("SELECT sku, unit_price, category FROM products ORDER BY sku")
            products = [Product(r[0], float(r[1]), r[2]) for r in cur.fetchall()]
            cur.execute("SELECT customer_id FROM customers ORDER BY customer_id")
            customers = [r[0] for r in cur.fetchall()]
        return cls(products, customers)


def diurnal_factor(dt: datetime) -> float:
    """Traffic curve: quiet at 04:00, peak around 20:00. Range ~0.35 - 1.6."""
    hour = dt.hour + dt.minute / 60.0
    return 0.95 + 0.65 * math.sin((hour - 10.0) / 24.0 * 2 * math.pi)


class OrderGenerator:
    def __init__(self, catalog: Catalog, seed: int | None = None):
        self.catalog = catalog
        self.rng = random.Random(seed)
        self.counter = 0
        self.recent_order_ids: list[str] = []
        # A per-process token in the order id. Without it every `inject` run
        # would restart the counter at 000001 and silently UPSERT over the
        # orders written by the previous run -- the table would stop growing
        # and the demo's numbers would quietly stop adding up. Deliberately
        # NOT derived from the seed: reproducible contents, unique keys.
        self.run_token = uuid.uuid4().hex[:4].upper()

    def next_order_id(self, ts: datetime) -> str:
        self.counter += 1
        return f"ORD-{ts.strftime('%Y%m%d')}-{self.run_token}-{self.counter:05d}"

    def make_order(self, ts: datetime | None = None) -> list[dict]:
        """One order -> a list of flat line records."""
        rng = self.rng
        ts = ts or now_utc()
        # spread order times over the last few minutes so time-series look natural
        order_ts = ts - timedelta(seconds=rng.randint(0, 240))
        order_id = self.next_order_id(order_ts)
        self.recent_order_ids.append(order_id)
        if len(self.recent_order_ids) > 500:
            self.recent_order_ids.pop(0)

        customer_id = rng.choice(self.catalog.customer_ids)
        status = rng.choice(STATUSES)
        channel = rng.choice(CHANNELS)
        payment = rng.choice(PAYMENTS)

        # basket size: mostly 1-2 lines, occasionally larger
        n_lines = rng.choices([1, 2, 3, 4, 5], weights=[45, 27, 15, 8, 5])[0]
        chosen = rng.sample(self.catalog.products, k=min(n_lines, len(self.catalog.products)))

        lines = []
        for i, product in enumerate(chosen, start=1):
            qty = rng.choices([1, 1, 1, 2, 2, 3, 4], weights=[35, 20, 12, 15, 8, 6, 4])[0]
            # small price jitter: promos and stale price lists happen in real data
            unit_price = round(product.unit_price * rng.uniform(0.97, 1.03), 2)
            lines.append({
                "order_id": order_id,
                "line_no": i,
                "customer_id": customer_id,
                # epoch millis for the pipeline, ISO string for human eyes.
                # Numbers survive schema inference cleanly; PutDatabaseRecord
                # converts once, at the JDBC boundary, against the real column.
                "order_ts": int(order_ts.timestamp() * 1000),
                "order_ts_iso": fmt_ts(order_ts),
                "status": status,
                "channel": channel,
                "payment_type": payment,
                "currency": "INR",
                "sku": product.sku,
                "qty": qty,
                "unit_price": unit_price,
                "line_total": round(unit_price * qty, 2),
            })
        return lines

    def make_batch(self, n_orders: int, ts: datetime | None = None) -> list[dict]:
        ts = ts or now_utc()
        records: list[dict] = []
        for _ in range(n_orders):
            records.extend(self.make_order(ts))
        return records

    def high_value_order(self, customer_id: str | None = None) -> list[dict]:
        """A deliberately expensive order: several units of the priciest SKUs.
        Used by the fraud_burst scenario to trip the HIGH_VALUE rule."""
        rng = self.rng
        pricey = sorted(self.catalog.products, key=lambda p: p.unit_price, reverse=True)[:10]
        lines = self.make_order()
        order_id = lines[0]["order_id"]
        customer_id = customer_id or lines[0]["customer_id"]
        out = []
        for i, product in enumerate(rng.sample(pricey, k=rng.randint(2, 4)), start=1):
            qty = rng.randint(3, 8)
            out.append({
                **lines[0],
                "line_no": i,
                "order_id": order_id,
                "customer_id": customer_id,
                "sku": product.sku,
                "qty": qty,
                "unit_price": product.unit_price,
                "line_total": round(product.unit_price * qty, 2),
                "status": "PAID",
                "channel": "WEB",
            })
        return out
