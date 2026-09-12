#!/usr/bin/env python3
"""Stream SQLite query results to JSONL.

The database is opened read-only and rows are serialised one at a time, so
exporting a hundred million rows costs the same memory as exporting ten.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import math
import re
import sqlite3
import sys
import time
from typing import Any, Iterator, TextIO

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ExportError(Exception):
    pass


def connect(path: str) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise ExportError(f"cannot open {path}: {exc}") from exc

    # A read-only export must never wait on or disturb a running collection.
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def quote_identifier(name: str) -> str:
    if not IDENTIFIER.match(name):
        raise ExportError(f"invalid table name {name!r}")
    return f'"{name}"'


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def build_query(conn: sqlite3.Connection, args: argparse.Namespace) -> str:
    if args.query:
        return args.query

    if not table_exists(conn, args.table):
        available = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        raise ExportError(
            f"no table or view named {args.table!r}. Available: {', '.join(available) or 'none'}"
        )

    query = f"SELECT * FROM {quote_identifier(args.table)}"
    if args.where:
        query += f" WHERE {args.where}"
    if args.order_by:
        query += f" ORDER BY {args.order_by}"
    return query


def convert(value: Any, column: str, json_columns: set[str], detect_json: bool) -> Any:
    if value is None or isinstance(value, (int, bool)):
        return value

    if isinstance(value, float):
        # JSON has no NaN or Infinity; emitting them produces files that other
        # parsers reject.
        return value if math.isfinite(value) else None

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"$base64": base64.b64encode(value).decode("ascii")}

    if isinstance(value, str):
        if column in json_columns or (detect_json and value[:1] in "{["):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                if column in json_columns:
                    raise
                return value
    return value


def stream(
    cursor: sqlite3.Cursor,
    out: TextIO,
    *,
    json_columns: set[str],
    detect_json: bool,
    batch: int,
    limit: int | None,
    progress: bool,
) -> int:
    columns = [d[0] for d in cursor.description or []]
    if not columns:
        raise ExportError("query returned no columns")

    written = 0
    started = time.monotonic()
    dumps = json.dumps

    while True:
        rows = cursor.fetchmany(batch)
        if not rows:
            break
        for row in rows:
            record = {
                col: convert(val, col, json_columns, detect_json)
                for col, val in zip(columns, row)
            }
            out.write(dumps(record, ensure_ascii=False, allow_nan=False))
            out.write("\n")
            written += 1

            if limit is not None and written >= limit:
                report(progress, written, started, final=True)
                return written
        report(progress, written, started)

    report(progress, written, started, final=True)
    return written


def report(enabled: bool, rows: int, started: float, final: bool = False) -> None:
    if not enabled:
        return
    elapsed = max(time.monotonic() - started, 1e-6)
    end = "\n" if final else "\r"
    print(
        f"{rows:,} rows  {rows / elapsed:,.0f} rows/sec",
        file=sys.stderr,
        end=end,
        flush=True,
    )


def open_output(path: str | None) -> tuple[TextIO, bool]:
    if not path or path == "-":
        return sys.stdout, False
    if path.endswith(".gz"):
        return gzip.open(path, "wt", encoding="utf-8", newline="\n"), True
    return open(path, "w", encoding="utf-8", newline="\n"), True


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="export.py",
        description="Stream SQLite query results to JSONL.",
        epilog=(
            "examples:\n"
            "  export.py massive.db --table rss_items --output items.jsonl\n"
            '  export.py massive.db --query "SELECT * FROM gdelt_events LIMIT 1000000" -o out.jsonl.gz\n'
            "  export.py massive.db --table github_events --detect-json | head"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", help="path to the SQLite database")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--query", "-q", help="SQL query to export")
    group.add_argument("--table", "-t", help="table or view to export")
    parser.add_argument("--where", help="WHERE clause used with --table")
    parser.add_argument("--order-by", help="ORDER BY clause used with --table")
    parser.add_argument("--output", "-o", help="output file (.gz compresses); default stdout")
    parser.add_argument("--limit", type=int, help="stop after N rows")
    parser.add_argument("--batch", type=int, default=1000, help="rows fetched per round trip")
    parser.add_argument(
        "--json-columns",
        default="",
        help="comma separated columns holding JSON to embed as objects",
    )
    parser.add_argument(
        "--detect-json",
        action="store_true",
        help="embed any string that parses as a JSON object or array",
    )
    parser.add_argument("--progress", action="store_true", help="report progress on stderr")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.batch < 1:
        print("error: --batch must be at least 1", file=sys.stderr)
        return 2
    if args.limit is not None and args.limit < 0:
        print("error: --limit cannot be negative", file=sys.stderr)
        return 2

    out = None
    close_out = False
    try:
        conn = connect(args.database)
        try:
            query = build_query(conn, args)
            try:
                cursor = conn.execute(query)
            except sqlite3.Error as exc:
                raise ExportError(f"{exc}\nquery: {query}") from exc

            out, close_out = open_output(args.output)
            rows = stream(
                cursor,
                out,
                json_columns={c.strip() for c in args.json_columns.split(",") if c.strip()},
                detect_json=args.detect_json,
                batch=args.batch,
                limit=args.limit,
                progress=args.progress,
            )
            if args.output and args.output != "-":
                print(f"wrote {rows:,} rows to {args.output}", file=sys.stderr)
        finally:
            conn.close()
    except ExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except BrokenPipeError:
        # Normal when piping into head; keep the shell's exit status quiet.
        try:
            sys.stdout.close()
        except Exception:
            pass
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        if out is not None and close_out:
            out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
