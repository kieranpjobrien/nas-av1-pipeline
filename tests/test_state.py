"""Tests for pipeline/state.py — SQLite-backed pipeline state management."""

import pytest

from pipeline.state import (
    ACTIVE_STATUSES,
    FLAGGED_STATUSES,
    TERMINAL_STATUSES,
    FileStatus,
    PipelineState,
    is_flagged,
    is_terminal,
)


class TestPipelineStateBasics:
    """Create, set, get, and round-trip file entries."""

    def test_create_empty_state(self, tmp_state_db):
        """A fresh state DB has zero tracked files."""
        state = PipelineState(tmp_state_db)
        assert state.get_all_files() == {}
        state.close()

    def test_set_and_get_file(self, tmp_state_db):
        """set_file persists an entry that get_file can retrieve."""
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Test.mkv"
        state.set_file(fp, FileStatus.PENDING, tier="H.264 1080p")
        entry = state.get_file(fp)
        assert entry is not None
        assert entry["status"] == "pending"
        assert entry["tier"] == "H.264 1080p"
        state.close()

    def test_get_file_missing_returns_none(self, tmp_state_db):
        """get_file for an untracked path returns None."""
        state = PipelineState(tmp_state_db)
        assert state.get_file("nonexistent") is None
        state.close()

    def test_set_file_preserves_existing_fields(self, tmp_state_db):
        """Updating a file's status preserves previously-set fields."""
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Test.mkv"
        state.set_file(fp, FileStatus.PENDING, tier="H.264 1080p", res_key="1080p")
        state.set_file(fp, FileStatus.FETCHING, stage="fetch")
        entry = state.get_file(fp)
        assert entry["status"] == "fetching"
        assert entry["tier"] == "H.264 1080p"
        assert entry["res_key"] == "1080p"
        assert entry["stage"] == "fetch"
        state.close()

    def test_extras_round_trip(self, tmp_state_db):
        """Non-direct columns are stored in the extras JSON blob and retrieved."""
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Extra.mkv"
        state.set_file(fp, FileStatus.PROCESSING, custom_field="hello", number=42)
        entry = state.get_file(fp)
        assert entry["custom_field"] == "hello"
        assert entry["number"] == 42
        state.close()

class TestStatusTransitions:
    """Verify the pipeline state machine transitions work correctly."""

    @pytest.mark.parametrize(
        "from_status,to_status",
        [
            (FileStatus.PENDING, FileStatus.FETCHING),
            (FileStatus.FETCHING, FileStatus.PROCESSING),
            (FileStatus.PROCESSING, FileStatus.UPLOADING),
            (FileStatus.UPLOADING, FileStatus.DONE),
            (FileStatus.PROCESSING, FileStatus.ERROR),
        ],
    )
    def test_status_transition(self, tmp_state_db, from_status, to_status):
        """Files can transition between valid pipeline states."""
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Transition.mkv"
        state.set_file(fp, from_status)
        state.set_file(fp, to_status)
        assert state.get_file(fp)["status"] == to_status.value
        state.close()

    def test_get_files_by_status(self, tmp_state_db):
        """get_files_by_status returns only matching filepaths."""
        state = PipelineState(tmp_state_db)
        state.set_file("a.mkv", FileStatus.PENDING)
        state.set_file("b.mkv", FileStatus.PROCESSING)
        state.set_file("c.mkv", FileStatus.PENDING)
        pending = state.get_files_by_status(FileStatus.PENDING)
        assert sorted(pending) == ["a.mkv", "c.mkv"]
        assert state.get_files_by_status(FileStatus.DONE) == []
        state.close()


class TestStats:
    """Stats tracking: completed count, bytes_saved, etc."""

    def test_default_stats(self, tmp_state_db):
        """A fresh state has zeroed stats."""
        state = PipelineState(tmp_state_db)
        assert state.stats["completed"] == 0
        assert state.stats["bytes_saved"] == 0
        assert state.stats["total_files"] == 0
        state.close()

    def test_update_stats_and_save(self, tmp_state_db):
        """Mutating stats in place and calling save() persists to DB."""
        state = PipelineState(tmp_state_db)
        state.stats["completed"] = 5
        state.stats["bytes_saved"] = 1_000_000_000
        state.save()

        # Re-open and verify persistence
        state2 = PipelineState(tmp_state_db)
        assert state2.stats["completed"] == 5
        assert state2.stats["bytes_saved"] == 1_000_000_000
        state.close()
        state2.close()

    def test_stats_setter(self, tmp_state_db):
        """Setting stats via the property setter replaces the whole dict."""
        state = PipelineState(tmp_state_db)
        state.stats = {"completed": 10, "bytes_saved": 500, "total_files": 10, "skipped": 0, "errors": 0}
        state.save()

        state2 = PipelineState(tmp_state_db)
        assert state2.stats["completed"] == 10
        state.close()
        state2.close()


class TestCompact:
    """compact() removes replaced/skipped entries."""

    def test_compact_removes_terminal_entries(self, tmp_state_db):
        """Entries with 'replaced' or 'skipped' status are removed by compact()."""
        state = PipelineState(tmp_state_db)
        state.set_file("keep.mkv", FileStatus.PENDING)
        state.set_file("done.mkv", FileStatus.DONE)
        # Manually insert replaced/skipped status for compact testing
        state._conn.execute(
            "INSERT OR REPLACE INTO pipeline_files (filepath, status) VALUES (?, ?)", ("rep.mkv", "replaced")
        )
        state._conn.execute(
            "INSERT OR REPLACE INTO pipeline_files (filepath, status) VALUES (?, ?)", ("skip.mkv", "skipped")
        )
        state._conn.commit()

        removed = state.compact()
        assert removed == 2
        assert state.get_file("rep.mkv") is None
        assert state.get_file("skip.mkv") is None
        assert state.get_file("keep.mkv") is not None
        assert state.get_file("done.mkv") is not None
        state.close()

    def test_compact_no_op_when_empty(self, tmp_state_db):
        """compact() returns 0 when there are no terminal entries."""
        state = PipelineState(tmp_state_db)
        state.set_file("active.mkv", FileStatus.PROCESSING)
        assert state.compact() == 0
        state.close()


class TestDataProperty:
    """The .data property returns the full state dict matching legacy JSON format."""

    def test_data_has_required_keys(self, tmp_state_db):
        """The data property includes created, last_updated, config, stats, files."""
        state = PipelineState(tmp_state_db)
        state.set_meta("created", "2026-01-01T00:00:00")
        data = state.data
        assert "created" in data
        assert "stats" in data
        assert "files" in data
        assert "config" in data
        state.close()

    def test_data_files_match_set_entries(self, tmp_state_db):
        """The files dict in .data contains all set file entries."""
        state = PipelineState(tmp_state_db)
        state.set_file("a.mkv", FileStatus.PENDING, tier="T1")
        state.set_file("b.mkv", FileStatus.DONE, tier="T2")
        data = state.data
        assert "a.mkv" in data["files"]
        assert "b.mkv" in data["files"]
        assert data["files"]["a.mkv"]["status"] == "pending"
        assert data["files"]["b.mkv"]["status"] == "done"
        state.close()

    def test_data_stats_reflect_mutations(self, tmp_state_db):
        """Stats mutations are visible in the .data property after save()."""
        state = PipelineState(tmp_state_db)
        state.stats["completed"] = 42
        state.save()
        assert state.data["stats"]["completed"] == 42
        state.close()


class TestStatusGroupings:
    """Membership of FileStatus values in the TERMINAL / ACTIVE / FLAGGED groups."""

    def test_terminal_includes_done_and_all_flagged(self):
        """DONE and every FLAGGED_* are terminal — queue builder skips them."""
        assert FileStatus.DONE in TERMINAL_STATUSES
        assert FileStatus.FLAGGED_FOREIGN_AUDIO in TERMINAL_STATUSES
        assert FileStatus.FLAGGED_UNDETERMINED in TERMINAL_STATUSES
        assert FileStatus.FLAGGED_MANUAL in TERMINAL_STATUSES

    def test_terminal_excludes_error(self):
        """ERROR is NOT terminal — files in ERROR get retried on next queue build.

        Discipline rule: a file the pipeline failed on must come back next pass,
        not be silently skipped forever (that's how the 2026-04-23 incident lost
        65 files).
        """
        assert FileStatus.ERROR not in TERMINAL_STATUSES
        assert FileStatus.PENDING not in TERMINAL_STATUSES

    def test_active_covers_in_flight_states(self):
        """ACTIVE = currently mid-flight; orchestrator reaps these on startup."""
        for s in (
            FileStatus.QUALIFYING,
            FileStatus.FETCHING,
            FileStatus.PROCESSING,
            FileStatus.UPLOADING,
        ):
            assert s in ACTIVE_STATUSES

    def test_flagged_subset_of_terminal(self):
        """All FLAGGED_* are terminal but not vice versa (DONE is also terminal)."""
        assert FLAGGED_STATUSES.issubset(TERMINAL_STATUSES)
        assert FileStatus.DONE in TERMINAL_STATUSES
        assert FileStatus.DONE not in FLAGGED_STATUSES


class TestStatusHelpers:
    """is_terminal / is_flagged accept both string and FileStatus values."""

    def test_is_terminal_accepts_string(self):
        assert is_terminal("done") is True
        assert is_terminal("flagged_foreign_audio") is True
        assert is_terminal("pending") is False
        assert is_terminal("error") is False

    def test_is_terminal_accepts_filestatus(self):
        assert is_terminal(FileStatus.DONE) is True
        assert is_terminal(FileStatus.FLAGGED_UNDETERMINED) is True
        assert is_terminal(FileStatus.PENDING) is False

    def test_is_terminal_unknown_string_is_false(self):
        """Garbage status string returns False rather than raising — defensive."""
        assert is_terminal("bogus") is False
        assert is_terminal("") is False

    def test_is_flagged_only_flagged(self):
        assert is_flagged("flagged_foreign_audio") is True
        assert is_flagged(FileStatus.FLAGGED_MANUAL) is True
        assert is_flagged("done") is False
        assert is_flagged("error") is False
        assert is_flagged(FileStatus.PENDING) is False


class TestNewStatusRoundTrip:
    """The new statuses persist through SQLite and can be retrieved unchanged."""

    def test_qualifying_persists(self, tmp_state_db):
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Series\Bluey\Season 1\Bluey S01E01.mkv"
        state.set_file(fp, FileStatus.QUALIFYING, mode="qualify")
        entry = state.get_file(fp)
        assert entry["status"] == "qualifying"
        state.close()

    def test_flagged_foreign_audio_persists(self, tmp_state_db):
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Series\Bluey\Season 1\Bluey S01E04 Daddy Robot.mkv"
        # Real-world example: Swedish dub of a show whose original_language is en
        state.set_file(
            fp,
            FileStatus.FLAGGED_FOREIGN_AUDIO,
            reason="audio detected as sv (0.84), original is en",
            stage="qualify",
        )
        entry = state.get_file(fp)
        assert entry["status"] == "flagged_foreign_audio"
        assert "sv" in entry["reason"]
        state.close()


class TestResetNonTerminal:
    """reset_non_terminal must preserve every TERMINAL_STATUS, not just DONE.

    Regression: 2026-04-26. The earlier SQL excluded only ('done', 'pending'),
    so on every pipeline restart all FLAGGED_* rows silently flipped back to
    PENDING, the queue builder picked them up, and the encode loop produced
    AV1 files with the same wrong audio the audit had just flagged. ~942 files
    were affected before the bug was caught.
    """

    def test_preserves_done(self, tmp_state_db):
        state = PipelineState(tmp_state_db)
        state.set_file("done.mkv", FileStatus.DONE)
        state.reset_non_terminal()
        assert state.get_file("done.mkv")["status"] == "done"
        state.close()

    def test_preserves_all_flagged_statuses(self, tmp_state_db):
        """Every FLAGGED_* must survive a reset — they are deliberate end states."""
        state = PipelineState(tmp_state_db)
        state.set_file(
            "foreign.mkv",
            FileStatus.FLAGGED_FOREIGN_AUDIO,
            reason="no audio matches original_language=en (detected: a:0=sv)",
        )
        state.set_file(
            "und.mkv",
            FileStatus.FLAGGED_UNDETERMINED,
            reason="audio is `und` and whisper couldn't identify it",
        )
        state.set_file("manual.mkv", FileStatus.FLAGGED_MANUAL, reason="user parked")

        state.reset_non_terminal()

        assert state.get_file("foreign.mkv")["status"] == "flagged_foreign_audio"
        assert state.get_file("und.mkv")["status"] == "flagged_undetermined"
        assert state.get_file("manual.mkv")["status"] == "flagged_manual"
        # And reasons must survive — recovery depends on them
        assert "sv" in state.get_file("foreign.mkv")["reason"]
        assert "whisper" in state.get_file("und.mkv")["reason"]
        state.close()

    def test_resets_active_states_to_pending(self, tmp_state_db):
        """PROCESSING / FETCHING / UPLOADING / QUALIFYING all reset to PENDING."""
        state = PipelineState(tmp_state_db)
        state.set_file("p.mkv", FileStatus.PROCESSING)
        state.set_file("f.mkv", FileStatus.FETCHING)
        state.set_file("u.mkv", FileStatus.UPLOADING)
        state.set_file("q.mkv", FileStatus.QUALIFYING)

        reset_count = state.reset_non_terminal()

        assert reset_count == 4
        for name in ("p.mkv", "f.mkv", "u.mkv", "q.mkv"):
            assert state.get_file(name)["status"] == "pending"
        state.close()

    def test_resets_error_to_pending(self, tmp_state_db):
        """ERROR is NOT terminal — gets retried by going back to PENDING."""
        state = PipelineState(tmp_state_db)
        state.set_file("err.mkv", FileStatus.ERROR, error="ffmpeg rc=1")
        state.reset_non_terminal()
        assert state.get_file("err.mkv")["status"] == "pending"
        state.close()

    def test_returncount_matches_reset_rows(self, tmp_state_db):
        """Mixed bag: only non-terminal rows are counted toward the return value."""
        state = PipelineState(tmp_state_db)
        state.set_file("done.mkv", FileStatus.DONE)
        state.set_file("flag.mkv", FileStatus.FLAGGED_FOREIGN_AUDIO)
        state.set_file("pend.mkv", FileStatus.PENDING)
        state.set_file("proc.mkv", FileStatus.PROCESSING)  # only this one resets

        assert state.reset_non_terminal() == 1
        state.close()

    def test_flagged_undetermined_persists(self, tmp_state_db):
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Mystery (1995).mkv"
        state.set_file(
            fp,
            FileStatus.FLAGGED_UNDETERMINED,
            reason="whisper exhausted (3 stages); audio remains und",
        )
        entry = state.get_file(fp)
        assert entry["status"] == "flagged_undetermined"
        state.close()
