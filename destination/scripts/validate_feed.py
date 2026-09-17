#!/usr/bin/env python3
"""Check a file is feedable BEFORE either engine sees it.

    python3 scripts/validate_feed.py myfile.json

Both engines pick up `*.json` from their landing directory and read it as
NDJSON -- ONE JSON OBJECT PER LINE. The `.json` extension is therefore a lie
about the contents, and it is the single most likely thing to be wrong in a
file somebody else generated: asked for "a JSON file", most tools produce a
JSON ARRAY, which his runner rejects line by line as PARSE_ERROR and which
would make a whole run meaningless. That is checked first, and hard.

Everything else here is a warning, not an error. A test file is SUPPOSED to
contain faults -- the point is to see them before the run, so you know what
each engine should reject and can tell a generator bug from an engine bug.
"""
import collections
import json
import sys
import time

FIELDS = ["order_id", "line_no", "customer_id", "order_ts", "order_ts_iso",
          "status", "channel", "payment_type", "currency", "sku", "qty",
          "unit_price", "line_total"]

# The shared catalogue. Anything outside it is a referential fault by design,
# so these counts should match the unknown_sku / unknown_customer rates asked
# for -- if they are much higher, the generator used the wrong catalogue and
# every UNKNOWN_* verdict in the run will be noise.
SKUS = {f"SKU-{i:04d}" for i in range(1, 19)}
CUSTOMERS = {f"CUST-{i:05d}" for i in range(1, 61)}
WINDOW_MS = 365 * 86400 * 1000


def main(path):
    counts = collections.Counter()
    lines = 0
    orders = set()
    keys = set()
    dups = 0
    now = time.time() * 1000

    with open(path, encoding="utf-8", errors="replace") as fh:
        head = fh.readline().lstrip()
        if head.startswith("["):
            sys.exit("REFUSING: this is a JSON ARRAY.\n"
                     "  Both engines read ONE JSON OBJECT PER LINE (NDJSON).\n"
                     "  Re-generate without the wrapping [ ] and without commas\n"
                     "  between records.")
        fh.seek(0)
        for line in fh:
            if not line.strip():
                counts["blank line"] += 1
                continue
            lines += 1
            try:
                r = json.loads(line)
            except Exception:
                counts["UNPARSEABLE (file-level fault - see note)"] += 1
                continue
            orders.add(r.get("order_id"))
            k = (r.get("order_id"), r.get("line_no"))
            if k in keys:
                dups += 1
            keys.add(k)
            for f in FIELDS:
                if f not in r:
                    counts[f"missing field: {f}"] += 1
            if r.get("sku") not in SKUS:
                counts["sku not in catalogue -> UNKNOWN_SKU"] += 1
            if r.get("customer_id") not in CUSTOMERS:
                counts["customer not in catalogue -> UNKNOWN_CUSTOMER"] += 1
            ts = r.get("order_ts")
            if not isinstance(ts, int):
                counts["order_ts not an integer -> PARSE_ERROR"] += 1
            elif abs(ts - now) > WINDOW_MS:
                counts["order_ts outside +/-365d -> BAD_TIMESTAMP"] += 1
            if not isinstance(r.get("qty"), int):
                counts["qty not an integer -> PARSE_ERROR"] += 1
            elif not 1 <= r["qty"] <= 999:
                counts["qty outside 1..999 -> RANGE_VIOLATION"] += 1
            up = r.get("unit_price")
            if not isinstance(up, (int, float)):
                counts["unit_price not a number -> PARSE_ERROR"] += 1
            elif not 0.01 <= up <= 9999.99:
                counts["unit_price outside range -> RANGE_VIOLATION"] += 1
            if str(r.get("currency", "")).upper() not in ("INR", "USD", "EUR", "GBP"):
                counts["currency not INR/USD/EUR/GBP -> BAD_CURRENCY"] += 1

    if not lines:
        sys.exit("REFUSING: no records at all. An empty file is a file-level "
                 "fault and the two engines handle it differently by design.")

    print(f"{path}")
    print(f"  {lines} lines, {len(orders)} orders, "
          f"{lines / max(len(orders), 1):.2f} lines per order")
    if dups:
        print(f"  {dups} repeated (order_id, line_no) -> DUP_KEY")

    if counts["UNPARSEABLE (file-level fault - see note)"]:
        print("\n  !! this file contains unparseable lines. That is a FILE-level")
        print("     fault: his engine rejects them one by one, NiFi fails the")
        print("     whole file. Keep those in a separate tiny file or the run")
        print("     tells you nothing.")

    if counts:
        print("\nexpected rejects -- check these are the faults you asked for:")
        total = 0
        for k, v in counts.most_common():
            print(f"  {v:>8}  {k}")
            total += v
        print(f"  {total:>8}  (upper bound; a record breaking two rules is")
        print("            reported once, under the FIRST rule reached)")
    else:
        print("\n  no faults at all -- unusual for a test file, but valid.")
    print("\nfeed it with:  make feed FILE=" + path)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python3 scripts/validate_feed.py <file.json>")
    main(sys.argv[1])
