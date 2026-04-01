import json
import os
import sys


class BedrockJob:
    """
    Runtime helper for Bedrock analysis jobs running inside a K8s container.

    Environment variables (injected by the query engine at job creation):
        BEDROCK_TOKEN           — short-lived Polaris token scoped to the submitting user
        BEDROCK_JOB_ID          — UUID of this job run
        BEDROCK_CATALOG_URL     — Polaris REST catalog base URL
        BEDROCK_OUTPUT_PATH     — s3:// prefix for writing Parquet output files
        BEDROCK_R2_ACCESS_KEY   — R2 access key (for DuckDB COPY TO)
        BEDROCK_R2_SECRET_KEY   — R2 secret key
        BEDROCK_R2_ACCOUNT_ID   — R2 account ID
    """

    def __init__(self):
        self.token = os.environ["BEDROCK_TOKEN"]
        self.job_id = os.environ["BEDROCK_JOB_ID"]
        self.catalog_url = os.environ["BEDROCK_CATALOG_URL"]
        # Polaris OAuth2 credential in "client_id:client_secret" format.
        # Injected by the runner; used by connect() to ATTACH the Iceberg catalog.
        self._catalog_credential = os.environ.get("BEDROCK_CATALOG_CREDENTIAL", "")

    @property
    def output_path(self) -> str:
        """S3 prefix for writing Parquet output files, e.g. s3://bedrock-lake/analytics/demo/{job_id}/data"""
        return os.environ["BEDROCK_OUTPUT_PATH"]

    def _fetch_polaris_token(self) -> str:
        """Exchange BEDROCK_CATALOG_CREDENTIAL for a short-lived Polaris bearer token."""
        import urllib.request, urllib.parse
        base = self.catalog_url.rstrip("/").removesuffix("/api/catalog")
        token_url = f"{base}/api/catalog/v1/oauth/tokens"
        client_id, _, client_secret = self._catalog_credential.partition(":")
        body = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        }).encode()
        req = urllib.request.Request(token_url, data=body, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            import json
            return json.load(resp)["access_token"]

    def connect(self):
        """
        Return a DuckDB in-memory connection with the Iceberg catalog attached.

        The connection is configured with:
          - iceberg + httpfs extensions loaded
          - R2 credentials set via BEDROCK_R2_* env vars (optional; Fluent Bit
            sidecar uses its own credentials)
          - Polaris catalog ATTACHed as 'catalog' using BEDROCK_TOKEN
        """
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")

        # R2 storage secret — used both for reading raw files and COPY TO output.
        r2_key = os.environ.get("BEDROCK_R2_ACCESS_KEY")
        r2_secret = os.environ.get("BEDROCK_R2_SECRET_KEY")
        r2_account = os.environ.get("BEDROCK_R2_ACCOUNT_ID")
        if r2_key and r2_secret and r2_account:
            conn.execute(f"""
                CREATE SECRET bedrock_r2 (
                    TYPE S3,
                    KEY_ID '{r2_key}',
                    SECRET '{r2_secret}',
                    ENDPOINT '{r2_account}.r2.cloudflarestorage.com',
                    URL_STYLE 'path'
                )
            """)

        # Fetch a short-lived bearer token from Polaris, then ATTACH using
        # ENDPOINT + TOKEN (the pattern DuckDB 1.5 requires for Iceberg REST).
        token = self._fetch_polaris_token()
        warehouse = os.environ.get("BEDROCK_CATALOG_WAREHOUSE", "bedrock")
        conn.execute(f"""
            ATTACH '{warehouse}' AS catalog (
                TYPE ICEBERG,
                ENDPOINT '{self.catalog_url}',
                TOKEN '{token}'
            )
        """)
        return conn

    # ── Output methods — all emit structured JSONL to stdout ────────────────

    def _emit(self, obj: dict):
        print(json.dumps(obj), flush=True)

    def update_progress(self, status: str, **kwargs):
        """
        Update the run payload in Postgres and advance the flow diagram.

        status:  flow-diagram step name, e.g. 'running_analysis', 'analysis_complete'
        kwargs:  optional extra fields merged into payload, e.g.
                     progress_pct=50, progress_message="Computing…", lineage={…}

        The runner intercepts lines with type='payload' and merges them into
        the analysis_runs.payload JSONB column in Postgres.
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
        """
        Emit a diagram.

        Args:
            format:  Diagram type, e.g. "mermaid".
            id:      Unique identifier for this diagram in the report.
            content: Raw diagram source (Mermaid graph definition, etc.).
        """
        self._emit({"type": "diagram", "format": format, "id": id, "content": content})

    def conclusion(self, text: str):
        """Emit a free-text conclusion paragraph."""
        self._emit({"type": "conclusion", "text": text})

    def complete(self):
        """Signal successful job completion. Must be the last call."""
        self._emit({"type": "status", "state": "complete"})
