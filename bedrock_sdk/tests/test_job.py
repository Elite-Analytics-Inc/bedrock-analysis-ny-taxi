import json
import os
import sys
from io import StringIO
from unittest.mock import patch

import pytest

# Inject required env vars before importing BedrockJob
os.environ.setdefault("BEDROCK_TOKEN", "test-token")
os.environ.setdefault("BEDROCK_JOB_ID", "00000000-0000-0000-0000-000000000001")
os.environ.setdefault("BEDROCK_CATALOG_URL", "http://polaris:18181/api/catalog")

from bedrock_sdk.job import BedrockJob


# ── helpers ────────────────────────────────────────────────────────────────

def capture(fn, *args, **kwargs) -> list[dict]:
    """Call fn(*args, **kwargs), capture stdout, parse each line as JSON."""
    buf = StringIO()
    with patch("sys.stdout", buf):
        fn(*args, **kwargs)
    lines = [l for l in buf.getvalue().splitlines() if l.strip()]
    return [json.loads(l) for l in lines]


@pytest.fixture
def job():
    return BedrockJob()


# ── constructor ────────────────────────────────────────────────────────────

def test_constructor_reads_env(job):
    assert job.token == "test-token"
    assert job.job_id == "00000000-0000-0000-0000-000000000001"
    assert job.catalog_url == "http://polaris:18181/api/catalog"


def test_constructor_missing_env():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(KeyError):
            BedrockJob()


# ── progress ───────────────────────────────────────────────────────────────

def test_progress(job):
    [msg] = capture(job.progress, 50, "halfway there")
    assert msg == {"type": "progress", "pct": 50, "message": "halfway there"}


def test_progress_zero(job):
    [msg] = capture(job.progress, 0, "starting")
    assert msg["pct"] == 0


def test_progress_hundred(job):
    [msg] = capture(job.progress, 100, "done")
    assert msg["pct"] == 100


# ── table ──────────────────────────────────────────────────────────────────

def test_table(job):
    [msg] = capture(job.table, "trips", "Monthly Trips", ["month", "count"], [["Jan", 1200], ["Feb", 950]])
    assert msg["type"] == "table"
    assert msg["id"] == "trips"
    assert msg["title"] == "Monthly Trips"
    assert msg["headers"] == ["month", "count"]
    assert msg["rows"] == [["Jan", 1200], ["Feb", 950]]


def test_table_empty_rows(job):
    [msg] = capture(job.table, "empty", "Empty", ["col"], [])
    assert msg["rows"] == []


# ── diagram ────────────────────────────────────────────────────────────────

def test_diagram(job):
    [msg] = capture(job.diagram, "mermaid", "flow", "graph LR; A-->B")
    assert msg == {"type": "diagram", "format": "mermaid", "id": "flow", "content": "graph LR; A-->B"}


# ── conclusion ─────────────────────────────────────────────────────────────

def test_conclusion(job):
    [msg] = capture(job.conclusion, "Trip volume rose 18% YoY.")
    assert msg == {"type": "conclusion", "text": "Trip volume rose 18% YoY."}


# ── complete ───────────────────────────────────────────────────────────────

def test_complete(job):
    [msg] = capture(job.complete)
    assert msg == {"type": "status", "state": "complete"}


# ── JSONL validity — all output must be valid JSON on a single line ─────────

def test_all_outputs_are_single_line_json(job):
    buf = StringIO()
    with patch("sys.stdout", buf):
        job.progress(10, "step 1")
        job.table("t", "T", ["a"], [[1]])
        job.diagram("mermaid", "d", "graph LR; X-->Y")
        job.conclusion("done")
        job.complete()
    lines = [l for l in buf.getvalue().splitlines() if l.strip()]
    assert len(lines) == 5
    for line in lines:
        obj = json.loads(line)   # raises if invalid JSON
        assert "type" in obj
