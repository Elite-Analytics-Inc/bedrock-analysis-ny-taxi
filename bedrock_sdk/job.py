import json
import os
import sys
import tempfile
from datetime import datetime, timezone


class BedrockJob:
    """
    Runtime helper for Bedrock analysis jobs running inside a K8s container.

    Environment variables (injected by the query engine at job creation):
        BEDROCK_JOB_TOKEN           — job-scoped JWT (user_id + roles + job_id)
        BEDROCK_JOB_ID              — UUID of this job run
        BEDROCK_QUERY_ENGINE_URL    — query engine HTTP URL (e.g. http://bedrock-query-engine:7777)

    Security model:
        - Reads: routed through the query engine's HTTP /query endpoint (ABAC enforced)
        - Writes: presigned PUT URLs from the query engine (path-scoped, time-limited)
        - No R2 credentials or Polaris credentials are exposed to the container
    """

    def __init__(self):
        self.job_token = os.environ["BEDROCK_JOB_TOKEN"]
        self.job_id = os.environ["BEDROCK_JOB_ID"]
        self.qe_url = os.environ.get("BEDROCK_QUERY_ENGINE_URL", "http://bedrock-query-engine:7777")
        self._conn = None  # lazy local DuckDB connection
        self._log_buffer = []  # accumulated JSONL lines for R2 upload
        self._last_flush = 0  # index of last flushed line

    def _local_conn(self):
        """Get or create a local DuckDB in-memory connection for processing."""
        if self._conn is None:
            import duckdb
            self._conn = duckdb.connect(":memory:")
        return self._conn

    def _http_headers(self):
        return {
            "Authorization": f"Bearer {self.job_token}",
            "Content-Type": "application/json",
        }

    def connect(self):
        """
        Return a local DuckDB in-memory connection.

        Use fetch() to load data from Iceberg tables (ABAC enforced via the query engine).
        The returned connection is for local processing only — it has no direct access
        to Iceberg or R2.
        """
        return self._local_conn()

    def fetch(self, table_name: str, sql: str):
        """
        Query Iceberg data through the query engine (ABAC enforced).

        Executes `sql` on the query engine's HTTP /query endpoint, then registers
        the result as a local DuckDB table named `table_name`.

        Example:
            job.fetch("trips", "SELECT * FROM catalog.transportation.nyc_taxi_trips WHERE year = 2022")
            result = conn.execute("SELECT COUNT(*) FROM trips").fetchone()
        """
        import urllib.request

        payload = json.dumps({"sql": sql}).encode()
        req = urllib.request.Request(
            f"{self.qe_url}/query",
            data=payload,
            method="POST",
            headers=self._http_headers(),
        )
        with urllib.request.urlopen(req, timeout=300) as resp:
            data = json.load(resp)

        conn = self._local_conn()
        columns = data.get("columns", [])
        rows = data.get("rows", [])

        if not rows:
            col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" ({col_defs})')
            return

        # Write JSON to temp file to avoid SQL injection from special chars.
        import tempfile
        tmp = os.path.join(tempfile.gettempdir(), f"_fetch_{table_name}.json")
        with open(tmp, "w") as f:
            json.dump(rows, f)
        conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM read_json_auto(\'{tmp}\')')
        os.remove(tmp)

    def write_parquet(self, name: str, sql: str):
        """
        Write SQL query results to a parquet file in the job's output location.

        Executes `sql` against the local DuckDB connection, writes to a temp file,
        then uploads via a presigned PUT URL from the query engine.

        Example:
            job.write_parquet("states", "SELECT state, avg_aqi FROM state_summary")
        """
        conn = self._local_conn()
        local_path = os.path.join(tempfile.gettempdir(), f"{name}.parquet")

        # Write parquet locally
        conn.execute(f"COPY ({sql}) TO '{local_path}' (FORMAT PARQUET)")

        # Get presigned PUT URL from query engine
        presigned_url = self._presign_upload(f"{name}.parquet")

        # Upload via HTTP PUT
        self._upload_file(local_path, presigned_url)

        # Report and clean up
        row_count = conn.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        print(f"  wrote {name}.parquet ({row_count} rows)", flush=True)
        os.remove(local_path)

    def write_parquet_rows(self, name: str, rows: list, columns: list):
        """
        Write raw row data to a parquet file (fallback for data not in DuckDB).

        Prefer write_parquet(name, sql) when data is already in local DuckDB tables.
        """
        conn = self._local_conn()
        col_defs = ", ".join(f'v[{i}] AS "{c}"' for i, c in enumerate(columns))
        json_str = json.dumps(rows, default=str, ensure_ascii=False)
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _write_tmp AS
            SELECT {col_defs} FROM (SELECT unnest('{json_str}'::JSON[] ) AS v)
        """)
        self.write_parquet(name, "SELECT * FROM _write_tmp")
        conn.execute("DROP TABLE IF EXISTS _write_tmp")

    def _presign_upload(self, filename: str) -> str:
        """Request a presigned PUT URL from the query engine."""
        import urllib.request

        url = f"{self.qe_url}/analysis/{self.job_id}/presign/{filename}"
        req = urllib.request.Request(url, method="GET", headers=self._http_headers())
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
        return data["url"]

    def _upload_file(self, local_path: str, presigned_url: str):
        """Upload a file to R2 via a presigned PUT URL."""
        import urllib.request

        with open(local_path, "rb") as f:
            file_data = f.read()

        req = urllib.request.Request(
            presigned_url,
            data=file_data,
            method="PUT",
            headers={"Content-Type": "application/octet-stream"},
        )
        with urllib.request.urlopen(req, timeout=300) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(f"Upload failed: HTTP {resp.status}")

    # ── Output methods — emit JSONL to stdout + buffer for R2 upload ────────

    def _emit(self, obj: dict):
        obj["ts"] = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = json.dumps(obj)
        print(line, flush=True)
        self._log_buffer.append(line)
        # Flush to R2: immediately on first emit, then every 5 lines.
        if self._last_flush == 0 or len(self._log_buffer) - self._last_flush >= 5:
            self._flush_logs()

    def _flush_logs(self):
        """Upload accumulated log lines to run.jsonl in R2 via presigned URL."""
        if not self._log_buffer:
            return
        try:
            url = self._presign_upload("run.jsonl")
            content = "\n".join(self._log_buffer) + "\n"
            import urllib.request
            req = urllib.request.Request(
                url, data=content.encode("utf-8"), method="PUT",
                headers={"Content-Type": "application/octet-stream"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                pass
            self._last_flush = len(self._log_buffer)
        except Exception as e:
            print(f"[warn] log flush failed: {e}", flush=True)

    def update_progress(self, status: str, **kwargs):
        """
        Update the run payload in Postgres and advance the flow diagram.

        status:  flow-diagram step name, e.g. 'running_analysis', 'analysis_complete'
        kwargs:  optional extra fields merged into payload, e.g.
                     progress_pct=50, progress_message="Computing…", lineage={…}
        """
        data = {"status": status, **kwargs}
        self._emit({"type": "payload", "data": data})

    def progress(self, pct: int, message: str):
        """Emit a progress update visible in the live log view. pct is 0–100."""
        self._emit({"type": "progress", "pct": pct, "message": message})

    def table(self, id: str, title: str, headers: list, rows: list):
        """Emit a named table result."""
        self._emit({"type": "table", "id": id, "title": title, "headers": headers, "rows": rows})

    def diagram(self, format: str, id: str, content: str):
        """Emit a diagram (e.g. Mermaid)."""
        self._emit({"type": "diagram", "format": format, "id": id, "content": content})

    def conclusion(self, text: str):
        """Emit a free-text conclusion paragraph."""
        self._emit({"type": "conclusion", "text": text})

    def complete(self):
        """Signal successful job completion. Must be the last call."""
        self._emit({"type": "status", "state": "complete"})
        self._flush_logs()  # Final flush — ensures all lines are in R2
