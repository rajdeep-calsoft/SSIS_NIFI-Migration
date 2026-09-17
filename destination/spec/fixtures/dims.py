#!/usr/bin/env python3
"""
The one catalogue both engines share.

Why this file exists: NiFi seeded 120 INR-scale products and 500 customers;
the SSIS side seeded 18 small-price products and 60 customers. Every
UNKNOWN_SKU / UNKNOWN_CUSTOMER verdict would have differed for free, before a
single rule was compared. So one list is canonical and both warehouses are
seeded from it.

The SSIS side is the reference for the migration, so its 18 products and 60
customers are taken verbatim -- same skus, same prices, same categories, same
emails. Everything added here is additive:

  * unit_cost      NiFi's products table requires it; derived, never compared.
  * country        NiFi's flow enriches orders with it (LookupRecord). The SSIS
    segment        customers table simply gains the two columns.
  * city           NiFi's customers table requires them.
    signup_date

  * names for CUST-00051..00060. His seed indexes a 50-element array with
    generate_series(1, 60), so the last ten customers get a NULL name. NiFi's
    full_name is NOT NULL, so they are filled in on both sides.

Run `make dims` to regenerate the two SQL files. Hand edits are lost.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
SSIS_REPO = os.getenv("SSIS_REPO", str(Path(__file__).resolve().parents[3] / "source"))

# --------------------------------------------------------------------------
# Taken verbatim from the SSIS side's scripts/stream_schema.sql:91-109.
# sku, unit_price, category, product_name, manufacturer
# --------------------------------------------------------------------------
PRODUCTS = [
    ("SKU-0001",  12.99, "electronics", "USB-C Hub",                "Aurora"),
    ("SKU-0002",  89.50, "electronics", "Bluetooth Speaker",        "Aurora"),
    ("SKU-0003", 249.00, "electronics", "Noise-Cancelling Headset", "Voltix"),
    ("SKU-0004",  32.25, "clothing",    "Zen T-Shirt",              "Threadline"),
    ("SKU-0005",  64.00, "clothing",    "Cloud Hoodie",             "Threadline"),
    ("SKU-0006",  18.40, "books",       "The Data Mindset",         "Papertrail"),
    ("SKU-0007",  27.90, "books",       "Streaming Systems 2e",     "Papertrail"),
    ("SKU-0008",  15.60, "home",        "Ceramic Mug Set",          "Homestead"),
    ("SKU-0009", 120.00, "home",        "Espresso Machine",         "Homestead"),
    ("SKU-0010",  22.80, "sports",      "Trail Bottle 1L",          "Pulsegear"),
    ("SKU-0011",  95.40, "sports",      "Resistance Band Kit",      "Pulsegear"),
    ("SKU-0012",  11.99, "toys",        "Stacking Blocks",          "Playlab"),
    ("SKU-0013",  34.75, "toys",        "Building Brick Set",       "Playlab"),
    ("SKU-0014",   7.95, "beauty",      "Lip Balm Trio",            "Glowco"),
    ("SKU-0015",  49.90, "beauty",      "Vitamin C Serum",          "Glowco"),
    ("SKU-0016",   3.20, "grocery",     "Organic Oats 500g",        "GreenFork"),
    ("SKU-0017",   8.75, "grocery",     "Cold Brew Can 4pk",        "GreenFork"),
    ("SKU-0018",   5.60, "grocery",     "Almond Milk 1L",           "GreenFork"),
]

# His 50-name array, verbatim (stream_schema.sql:120-131).
NAMES = [
    "Aarav Mehta", "Giulia Bianchi", "Noah Kim", "Priya Nair", "Liam O'Connor",
    "Sofia Rossi", "Kenji Tanaka", "Aisha Rahman", "Mateo Costa", "Ingrid Larsen",
    "Wei Zhang", "Fatima Haddad", "Diego Alvarez", "Mia Johansson", "Omar Farouk",
    "Hana Sato", "Emma Dubois", "Ravi Sharma", "Lena Novak", "Yusuf Demir",
    "Elena Petrova", "Arjun Chopra", "Camila Gomez", "Felix Weber", "Nina Kozlov",
    "David Chen", "Isabella Russo", "Raj Patel", "Lea Muller", "Marco Silva",
    "Anya Ivanova", "Sara Costa", "Tom Becker", "Zara Ali", "Lucas Meyer",
    "Mei Lin", "Olga Nowak", "Bruno Ferrari", "Eva Hansen", "Khalid Hassan",
    "Nora Kovacs", "Pablo Ruiz", "Freya Schmidt", "Amir Hussain", "Chloe Martin",
    "Daniel Okafor", "Sigrid Berg", "Hugo Lindqvist", "Ivy Thompson", "Jack Wilson",
]

N_CUSTOMERS = 60
COUNTRIES = ["IN", "IN", "IN", "IN", "US", "GB", "AE", "SG"]
CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad",
          "Pune", "Chennai", "Kolkata", "Ahmedabad"]


def customers() -> list[tuple]:
    """(customer_id, full_name, email, country, city, segment, signup_date)."""
    rows = []
    for n in range(1, N_CUSTOMERS + 1):
        # His array runs out at 50; NiFi's full_name is NOT NULL, so the tail
        # is filled deterministically rather than left NULL on either side.
        name = NAMES[n - 1] if n <= len(NAMES) else f"Customer {n:05d}"
        segment = ("WHOLESALE" if n % 25 == 0
                   else "PRIME" if n % 5 == 0
                   else "RETAIL")
        rows.append((
            f"CUST-{n:05d}",
            name,
            f"user{n}@streamstore.example",     # his email scheme, unchanged
            COUNTRIES[n % len(COUNTRIES)],
            CITIES[n % len(CITIES)],
            segment,
            (date(2021, 1, 1) + timedelta(days=(n * 3) % 1700)).isoformat(),
        ))
    return rows


def q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


HEADER = """\
-- =====================================================================
-- GENERATED by spec/fixtures/dims.py -- do not hand-edit.
--
-- The single catalogue shared by the NiFi and SSIS engines: 18 products
-- (3.20 - 249.00) and 60 customers, taken from the SSIS side. Both
-- warehouses must hold exactly these rows or UNKNOWN_SKU and
-- UNKNOWN_CUSTOMER cannot be compared.
-- =====================================================================
"""


def nifi_sql() -> str:
    out = [HEADER, "\nINSERT INTO products "
           "(sku, product_name, category, unit_price, unit_cost) VALUES"]
    rows = [f"    ({q(sku)}, {q(name)}, {q(cat)}, {price}, {round(price * 0.62, 2)})"
            for sku, price, cat, name, _mfr in PRODUCTS]
    out.append(",\n".join(rows) + "\nON CONFLICT (sku) DO NOTHING;\n")

    out.append("\nINSERT INTO customers "
               "(customer_id, full_name, email, country, city, segment, signup_date) VALUES")
    rows = [f"    ({q(cid)}, {q(name)}, {q(mail)}, {q(country)}, {q(city)}, "
            f"{q(seg)}, DATE {q(signup)})"
            for cid, name, mail, country, city, seg, signup in customers()]
    out.append(",\n".join(rows) + "\nON CONFLICT (customer_id) DO NOTHING;\n")
    return "\n".join(out)


def ssis_sql() -> str:
    out = [HEADER,
           "\n-- country/segment are ADDITIVE. No SSIS rule reads them; NiFi's",
           "-- enrich_customer stage does, and both warehouses must agree.",
           "ALTER TABLE public.customers ADD COLUMN IF NOT EXISTS country VARCHAR(8);",
           "ALTER TABLE public.customers ADD COLUMN IF NOT EXISTS segment VARCHAR(20);\n",
           "\nINSERT INTO public.products "
           "(sku, unit_price, category, product_name, manufacturer) VALUES"]
    rows = [f"    ({q(sku)}, {price}, {q(cat)}, {q(name)}, {q(mfr)})"
            for sku, price, cat, name, mfr in PRODUCTS]
    out.append(",\n".join(rows) + "\nON CONFLICT (sku) DO NOTHING;\n")

    out.append("\nINSERT INTO public.customers "
               "(customer_id, customer_name, email, country, segment) VALUES")
    rows = [f"    ({q(cid)}, {q(name)}, {q(mail)}, {q(country)}, {q(seg)})"
            for cid, name, mail, country, _city, seg, _signup in customers()]
    out.append(",\n".join(rows) + """
ON CONFLICT (customer_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    email         = EXCLUDED.email,
    country       = EXCLUDED.country,
    segment       = EXCLUDED.segment;
""")
    return "\n".join(out)


def main() -> int:
    for name, body in (("dims_nifi.sql", nifi_sql()), ("dims_ssis.sql", ssis_sql())):
        (HERE / name).write_text(body)
        print(f"wrote spec/fixtures/{name}")
    # db/init runs in name order at first boot; this IS the NiFi seed.
    (HERE.parent.parent / "db" / "init" / "02_seed_dims.sql").write_text(nifi_sql())
    print("wrote db/init/02_seed_dims.sql")

    # ...and deliver the SSIS side's copy, if that repo is where we expect.
    # It is a separate, non-git checkout, so this is a copy and not a symlink:
    # the other developer must be able to read and review it in place.
    ssis = Path(SSIS_REPO) / "scripts" / "shared_dims.sql"
    if ssis.parent.is_dir():
        ssis.write_text(ssis_sql())
        print(f"wrote {ssis}")
    else:
        print(f"SKIPPED {ssis} -- the SSIS repo is not at {SSIS_REPO}")
    print(f"\n{len(PRODUCTS)} products, {N_CUSTOMERS} customers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
