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
