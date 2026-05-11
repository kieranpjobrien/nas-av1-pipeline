"""Tests for server API endpoints via FastAPI TestClient."""


class TestHealthEndpoint:
    """GET /api/health returns system health data."""

    def test_health_returns_expected_fields(self, test_app):
        """Health endpoint returns a dict with expected keys."""
        resp = test_app.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        expected_fields = [
            "nas_movies_reachable",
            "nas_series_reachable",
            "staging_free_gb",
            "staging_total_gb",
            "ffmpeg_version",
            "gpu_available",
            "pipeline_status",
            "python_version",
        ]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

    def test_health_returns_python_version(self, test_app):
        """Health endpoint reports a non-empty Python version string."""
        resp = test_app.get("/api/health")
        data = resp.json()
        assert isinstance(data["python_version"], str)
        assert len(data["python_version"]) > 0


class TestPipelineEndpoint:
    """GET /api/pipeline when no state DB exists."""

    def test_pipeline_no_state(self, test_app):
        """When no pipeline state exists, returns status 'no_state'."""
        resp = test_app.get("/api/pipeline")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "no_state"


class TestConfigEndpoint:
    """GET /api/config returns pipeline configuration."""

    def test_config_returns_dict(self, test_app):
        """Config endpoint returns a dict with defaults, overrides, and effective."""
        resp = test_app.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "defaults" in data
        assert "overrides" in data
        assert "effective" in data
        assert isinstance(data["effective"], dict)

    def test_config_effective_has_cq(self, test_app):
        """The effective config includes CQ tables."""
        resp = test_app.get("/api/config")
        data = resp.json()
        assert "cq" in data["effective"]
        assert "movie" in data["effective"]["cq"]
        assert "series" in data["effective"]["cq"]


class TestProcessEndpoint:
    """POST /api/process/{name}/start with unknown process names."""

    def test_unknown_process_404(self, test_app):
        """Starting an unknown process returns 404."""
        resp = test_app.post("/api/process/unknown/start")
        assert resp.status_code == 404

    def test_unknown_process_status_404(self, test_app):
        """Getting status of an unknown process returns 404."""
        resp = test_app.get("/api/process/unknown/status")
        assert resp.status_code == 404

    def test_valid_process_status(self, test_app):
        """Getting status of a valid process name returns 200."""
        resp = test_app.get("/api/process/pipeline/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data


class TestControlEndpoints:
    """Control file read endpoints return sensible defaults."""

    def test_skip_default_empty(self, test_app):
        """GET /api/control/skip returns empty paths when no skip file exists."""
        resp = test_app.get("/api/control/skip")
        assert resp.status_code == 200
        data = resp.json()
        assert "paths" in data

    def test_priority_default_empty(self, test_app):
        """GET /api/control/priority returns defaults when no priority file exists."""
        resp = test_app.get("/api/control/priority")
        assert resp.status_code == 200
        data = resp.json()
        assert "force" in data
        assert "paths" in data

    def test_gentle_default(self, test_app):
        """GET /api/control/gentle returns defaults when no gentle file exists."""
        resp = test_app.get("/api/control/gentle")
        assert resp.status_code == 200
        data = resp.json()
        assert "default_offset" in data

    def test_control_status(self, test_app):
        """GET /api/control/status returns pause state and file existence flags."""
        resp = test_app.get("/api/control/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "pause_state" in data
        assert data["pause_state"] == "running"


class TestRequeueEndpoint:
    """POST /api/file/requeue and /api/files/requeue-batch must stamp
    ``force_reencode=true`` in the row's extras JSON. Without it,
    already-AV1 files queued via the dashboard would be silently
    skipped by the orchestrator's queue builder (categorise_entry
    routes AV1 files to gap_filler/skip based on codec, ignoring
    pending status). Pin this so the wiring doesn't regress.
    """

    def _make_test_file(self, subdir: str = "movies", name: str = "GradeNonOptimal.mkv") -> str:
        """Create a fake .mkv inside the test NAS dir so the endpoint's
        os.path.exists + path-prefix checks pass. Also init the state DB
        tables (the requeue endpoint uses raw sqlite3, not PipelineState,
        so it doesn't auto-create the schema)."""
        import os as _os

        nas = _os.environ["NAS_MOVIES"] if subdir == "movies" else _os.environ["NAS_SERIES"]
        path = _os.path.join(nas, name)
        with open(path, "wb") as f:
            f.write(b"x" * 1024)

        # Ensure pipeline_files table exists for raw-sqlite endpoint writes.
        from paths import PIPELINE_STATE_DB
        from pipeline.state import PipelineState

        state = PipelineState(str(PIPELINE_STATE_DB))
        state.close()
        return path

    def _read_extras(self, path: str) -> dict:
        import json as _json
        import sqlite3 as _sqlite3

        from paths import PIPELINE_STATE_DB

        con = _sqlite3.connect(str(PIPELINE_STATE_DB))
        try:
            row = con.execute(
                "SELECT extras FROM pipeline_files WHERE filepath = ?", (path,)
            ).fetchone()
        finally:
            con.close()
        if not row or not row[0]:
            return {}
        return _json.loads(row[0])

    def test_single_requeue_sets_force_reencode_for_new_row(self, test_app):
        """No existing row → INSERT path stamps force_reencode=true."""
        path = self._make_test_file()
        resp = test_app.post("/api/file/requeue", json={"path": path})
        assert resp.status_code == 200, resp.text
        extras = self._read_extras(path)
        assert extras.get("force_reencode") is True

    def test_single_requeue_sets_force_reencode_for_existing_done_row(self, test_app):
        """Existing DONE row → UPDATE path preserves other extras AND adds
        force_reencode=true. Simulates the realistic case: an already-
        encoded AV1 file the user wants re-encoded at a new CQ."""
        import sqlite3 as _sqlite3

        from paths import PIPELINE_STATE_DB

        path = self._make_test_file()
        # Pre-seed a DONE row with some prior extras to ensure they survive.
        con = _sqlite3.connect(str(PIPELINE_STATE_DB))
        con.execute(
            "INSERT OR REPLACE INTO pipeline_files (filepath, status, extras) "
            "VALUES (?, 'done', ?)",
            (path, '{"encode_params_used": {"cq": 32}}'),
        )
        con.commit()
        con.close()

        resp = test_app.post("/api/file/requeue", json={"path": path})
        assert resp.status_code == 200, resp.text
        extras = self._read_extras(path)
        assert extras.get("force_reencode") is True
        # Prior extras must survive — encode_params_used is the canonical
        # carry-forward field, losing it would cause the next encode to
        # forget the user's CQ override.
        assert extras.get("encode_params_used") == {"cq": 32}

    def test_batch_requeue_sets_force_reencode_on_all(self, test_app):
        """Bulk endpoint stamps the flag on every row it touches."""
        # Init schema + create files in one shot via the helper.
        paths = [
            self._make_test_file(name="BatchA.mkv"),
            self._make_test_file(name="BatchB.mkv"),
            self._make_test_file(name="BatchC.mkv"),
        ]

        resp = test_app.post("/api/files/requeue-batch", json={"paths": paths})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["queued"] == len(paths)
        for p in paths:
            assert self._read_extras(p).get("force_reencode") is True
