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
        - Query engine runs as a sidecar in the same pod (localhost:7777/7778)
        - Reads: routed through the sidecar's DuckDB + Iceberg (ABAC enforced)
        - Writes: presigned PUT URLs from the sidecar (path-scoped, time-limited)
        - Sidecar has Polaris credentials (ConfigMap) — analysis container does not
        - All traffic stays within the pod — no load on shared API replicas
    """

    def __init__(self):
        self.job_token = os.environ["BEDROCK_JOB_TOKEN"]
        self.job_id = os.environ["BEDROCK_JOB_ID"]
        self.qe_url = os.environ.get("BEDROCK_QUERY_ENGINE_URL", "http://bedrock-query-engine:7777")
        self._conn = None  # lazy local DuckDB connection
        self._log_buffer = []  # accumulated JSONL lines for R2 upload
        self._last_flush = 0  # index of last flushed line
        self._wait_for_sidecar()

    def _wait_for_sidecar(self):
        """Block until the query engine sidecar is reachable (up to 30s)."""
        if "localhost" not in self.qe_url:
            return  # not using sidecar
        import time
        import urllib.request
        for attempt in range(30):
            try:
                req = urllib.request.Request(f"{self.qe_url}/health", method="GET")
                with urllib.request.urlopen(req, timeout=2):
                    print(f"[sdk] sidecar ready after {attempt}s", flush=True)
                    return
            except Exception:
                time.sleep(1)
        print("[sdk] warning: sidecar not reachable after 30s, proceeding anyway", flush=True)

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

    def execute(self, sql: str):
        """
        Execute a DML/DDL statement on the query engine (INSERT, CREATE, etc.).

        Returns the JSON response (typically empty columns/rows for mutations).
        ABAC is enforced — the user's roles must grant access to the target table.
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
            return json.load(resp)

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

    def write_dashboard(self, local_path: str):
        """
        Upload a dashboard markdown file to R2 alongside the parquet outputs.

        The file lands at analytics/bedrock/<job_id>/dashboard/index.md and is
        rendered by the Bedrock Dash framework at request time — no Evidence
        build step needed.

        Typically called at the end of analysis.py:
            job.write_dashboard("dashboard/index.md")

        Args:
            local_path: path to the .md file relative to the analysis repo root
        """
        import os.path as _osp

        if not _osp.isfile(local_path):
            print(f"  [warn] dashboard file not found: {local_path}", flush=True)
            return

        # Upload as dashboard/index.md (the presign endpoint scopes to data/ prefix,
        # so we use the dashboard/ prefix via the filename)
        dest = "dashboard/" + _osp.basename(local_path)
        presigned_url = self._presign_upload(dest)
        self._upload_file(local_path, presigned_url)
        print(f"  wrote {dest} ({_osp.getsize(local_path)} bytes)", flush=True)

    def fetch_url_to_home(self, url: str, filename: str = None, max_bytes: int = 10 * 1024 * 1024 * 1024) -> str:
        """
        Fetch a public HTTP(S) URL and store it in the caller's home directory
        on R2 (`home/<user_id>/<filename>`).

        The download streams from inside the analysis container's own pod —
        no bytes touch the shared query-engine replicas. The sidecar issues a
        presigned PUT URL scoped to the caller's home dir (identity is taken
        from the job-scoped JWT), so this method can only ever write to the
        *caller's own* home — never another user's.

        Args:
            url: HTTP(S) URL to fetch. Must be http:// or https://.
                 Private/loopback/link-local hosts are rejected (SSRF guard).
            filename: Optional override for the destination filename.
                      Defaults to the sanitized basename of the URL path.
            max_bytes: Hard cap on download size. Default 10 GiB.

        Returns:
            The R2 path of the stored file, e.g. "home/alice/data.csv".

        Example:
            home_path = job.fetch_url_to_home("https://example.com/sample.parquet")
            job.fetch("t", f"SELECT * FROM read_parquet('s3://bedrock-lake/{home_path}')")
        """
        import ipaddress
        import os.path
        import re
        import socket
        import urllib.parse
        import urllib.request

        # 1. URL validation + SSRF guard
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"fetch_url_to_home: scheme must be http or https, got {parsed.scheme!r}")
        if not parsed.hostname:
            raise ValueError("fetch_url_to_home: URL has no hostname")

        try:
            for info in socket.getaddrinfo(parsed.hostname, None):
                addr = info[4][0]
                ip = ipaddress.ip_address(addr)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                    raise ValueError(f"fetch_url_to_home: refusing to fetch from non-public host {parsed.hostname} ({addr})")
        except socket.gaierror as e:
            raise ValueError(f"fetch_url_to_home: DNS resolution failed for {parsed.hostname}: {e}")

        # 2. Derive filename
        if not filename:
            filename = os.path.basename(parsed.path) or "downloaded"
        # Sanitize: no path traversal, no slashes
        filename = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
        if not filename or filename.startswith("."):
            filename = "downloaded_" + filename.lstrip(".")

        # 3. Stream download to a temp file with size cap
        local_path = os.path.join(tempfile.gettempdir(), f"_home_{filename}")
        bytes_read = 0
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "bedrock-sdk/1.0"})
            with urllib.request.urlopen(req, timeout=300) as resp, open(local_path, "wb") as out:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    if bytes_read > max_bytes:
                        raise ValueError(f"fetch_url_to_home: download exceeded max_bytes={max_bytes}")
                    out.write(chunk)
        except Exception:
            if os.path.exists(local_path):
                os.remove(local_path)
            raise

        # 4. Request presigned PUT for home/<user_id>/<filename>
        presigned = self._presign_home_upload(filename, bytes_read)

        # 5. Upload
        try:
            self._upload_file(local_path, presigned["url"])
        finally:
            os.remove(local_path)

        home_path = presigned.get("path") or f"home/<user>/{filename}"
        print(f"[sdk] fetched {url} → {home_path} ({bytes_read} bytes)", flush=True)
        return home_path

    def _presign_home_upload(self, filename: str, size: int) -> dict:
        """Request a presigned PUT URL scoped to the caller's home dir."""
        import urllib.request

        url = f"{self.qe_url}/home/presign"
        body = json.dumps({"filename": filename, "size": size}).encode()
        req = urllib.request.Request(url, data=body, method="POST", headers=self._http_headers())
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)

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
        obj["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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
            urllib.request.urlopen(req, timeout=30).close()
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
        # Signal sidecar to shut down via shared lifecycle volume
        try:
            with open("/lifecycle/done", "w") as f:
                f.write("done")
        except OSError:
            pass  # Not running with sidecar (e.g. local dev)
