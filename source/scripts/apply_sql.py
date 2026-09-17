"""Apply a .sql script via psycopg2.

Handles dollar-quoted bodies ($$...$$, $tag$...$tag$), single/double quoted
strings, line/block comments and multiple statements — so the same files can
run from the builder container without a psql binary.
"""
from __future__ import annotations

import re

DOLLAR_OPEN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")

# A chunk that is nothing but comments is not a statement. This matters for the
# text after the LAST semicolon: a file that ends with a trailing comment block
# used to yield one final "statement" made entirely of `--` lines, which
# Postgres rejects with "can't execute an empty query" -- and because
# tests/conftest.py asserts no statement failed, that turned every test in the
# suite into a setup error. Stripping comments before the emptiness test fixes
# it wherever it appears, not just at the end of one file.
_COMMENTS = re.compile(r"/\*.*?\*/|--[^\n]*", re.S)


def _is_only_comments(chunk):
    return not _COMMENTS.sub("", chunk).strip()


def split_statements(sql):
    statements = []
    i, n = 0, len(sql)
    buf = []
    state = "NORMAL"

    while i < n:
        ch = sql[i]
        two = sql[i:i + 2]

        if state == "LINE":
            if ch == "\n":
                state = "NORMAL"
            buf.append(ch)
            i += 1
            continue

        if state == "BLOCK":
            if two == "*/":
                buf.append(two)
                state = "NORMAL"
                i += 2
            else:
                buf.append(ch)
                i += 1
            continue

        if state == "SINGLE":
            buf.append(ch)
            if ch == "'" and sql[i + 1:i + 2] != "'":
                state = "NORMAL"
            i += 1
            continue

        if state == "DOUBLE":
            buf.append(ch)
            if ch == '"':
                state = "NORMAL"
            i += 1
            continue

        # NORMAL
        if two == "--":
            state = "LINE"
            buf.append(two)
            i += 2
            continue
        if two == "/*":
            state = "BLOCK"
            buf.append(two)
            i += 2
            continue
        if ch == "'":
            state = "SINGLE"
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            state = "DOUBLE"
            buf.append(ch)
            i += 1
            continue
        if ch == "$":
            m = DOLLAR_OPEN.match(sql, i)
            if m:
                tag = m.group(1) or ""
                close = f"${tag}$"
                end = sql.find(close, m.end())
                if end == -1:
                    buf.append(sql[i:])
                    i = n
                else:
                    buf.append(sql[i:end + len(close)])
                    i = end + len(close)
                continue
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt and not _is_only_comments(stmt):
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail and not _is_only_comments(tail):
        statements.append(tail)
    return statements


def apply_sql(conn, script_path, log=None):
    with open(script_path) as f:
        sql = f.read()
    stmts = split_statements(sql)
    cur = conn.cursor()
    failed = []
    for s in stmts:
        try:
            cur.execute(s)
            conn.commit()
        except Exception as e:  # noqa: BLE001 - report and continue
            conn.rollback()
            failed.append((s[:80], str(e)))
            if log:
                log.warning("statement failed: %s -> %s", s[:80], e)
    cur.close()
    return failed


def apply_sql_path(conn, script_path, log=None):
    return apply_sql(conn, script_path, log)

def main(argv=None):
    """Run a .sql file from the command line.

    Without this the module can only be imported, so
    `python3 scripts/apply_sql.py some.sql` silently did nothing at all --
    no output, no error, no statements run. Every failed statement is
    reported and the exit code counts them, so a caller can tell.
    """
    import argparse
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ssis_sim import simulator as S

    ap = argparse.ArgumentParser(description="Apply a .sql script to the warehouse.")
    ap.add_argument("script", help="path to the .sql file")
    args = ap.parse_args(argv)

    conn = S.conn()
    failed = apply_sql_path(conn, args.script)
    conn.close()

    name = os.path.basename(args.script)
    if failed:
        print(f"{name}: {len(failed)} statement(s) failed")
        for stmt, err in failed:
            print(f"  {stmt.strip()[:70]!r} -> {err.strip().splitlines()[0]}")
        return 1
    print(f"{name}: applied cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
