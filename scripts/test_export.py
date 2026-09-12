"""Tests for export.py — run with: pytest scripts"""

import gzip
import io
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import export  # noqa: E402


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "test.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name TEXT,
            score REAL,
            payload TEXT,
            blob BLOB,
            missing TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO items (id, name, score, payload, blob, missing) VALUES (?,?,?,?,?,?)",
        [
            (1, "first", 1.5, '{"a": 1}', b"\xff\xfe binary", None),
            (2, "sécond", 2.5, "[1,2,3]", b"plain text", None),
            (3, "third", 3.5, "not json", None, None),
        ],
    )
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
def run_export(database, tmp_path):
    def run(*args):
        out = tmp_path / "out.jsonl"
        assert export.main([database, "--output", str(out), *args]) == 0
        return [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]

    return run


def test_table_export(run_export):
    rows = run_export("--table", "items")
    assert len(rows) == 3
    assert rows[0]["name"] == "first"
    assert rows[1]["name"] == "sécond"
    assert rows[0]["missing"] is None
    assert rows[0]["score"] == 1.5


def test_query_export(run_export):
    rows = run_export("--query", "SELECT id, name FROM items WHERE id > 1 ORDER BY id")
    assert [r["id"] for r in rows] == [2, 3]
    assert set(rows[0]) == {"id", "name"}


def test_limit_and_where(run_export):
    rows = run_export("--table", "items", "--where", "id >= 2", "--limit", "1")
    assert len(rows) == 1
    assert rows[0]["id"] == 2


def test_blob_handling(run_export):
    rows = run_export("--table", "items")
    assert rows[1]["blob"] == "plain text"
    assert "$base64" in rows[0]["blob"]
    assert rows[2]["blob"] is None


def test_json_columns(run_export):
    rows = run_export("--table", "items", "--json-columns", "payload", "--limit", "2")
    assert rows[0]["payload"] == {"a": 1}
    assert rows[1]["payload"] == [1, 2, 3]


def test_detect_json_keeps_plain_strings(run_export):
    rows = run_export("--table", "items", "--detect-json")
    assert rows[0]["payload"] == {"a": 1}
    assert rows[2]["payload"] == "not json"


def test_gzip_output(database, tmp_path):
    out = tmp_path / "out.jsonl.gz"
    assert export.main([database, "--table", "items", "--output", str(out)]) == 0
    with gzip.open(out, "rt", encoding="utf-8") as fh:
        assert len(fh.readlines()) == 3


def test_stdout(database, monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    assert export.main([database, "--table", "items", "--limit", "1"]) == 0
    assert json.loads(buf.getvalue())["id"] == 1


@pytest.mark.parametrize(
    "args",
    [
        ["--table", "nope"],
        ["--query", "SELECT * FROM nope"],
    ],
)
def test_bad_target_is_reported(database, args):
    assert export.main([database, *args]) == 1


def test_missing_database_is_reported(tmp_path):
    assert export.main([str(tmp_path / "absent.db"), "--table", "x"]) == 1


def test_database_is_opened_read_only(database):
    conn = export.connect(database)
    with pytest.raises(sqlite3.Error):
        conn.execute("DELETE FROM items")
    conn.close()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_become_null(value):
    assert export.convert(value, "c", set(), False) is None


def test_invalid_identifier_rejected():
    with pytest.raises(export.ExportError):
        export.quote_identifier("items; DROP TABLE items")


def test_streaming_does_not_materialise_rows():
    # A generator-backed cursor stands in for a huge result set: if the exporter
    # buffered rows, memory would grow with row count.
    class FakeCursor:
        description = [("n",)]

        def __init__(self, total):
            self.remaining = total

        def fetchmany(self, size):
            take = min(size, self.remaining)
            self.remaining -= take
            return [(i,) for i in range(take)]

    sink = io.StringIO()
    written = export.stream(
        FakeCursor(50_000),
        sink,
        json_columns=set(),
        detect_json=False,
        batch=1000,
        limit=None,
        progress=False,
    )
    assert written == 50_000
    assert sink.getvalue().count("\n") == 50_000
