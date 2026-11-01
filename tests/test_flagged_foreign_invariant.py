"""A flagged_foreign_audio row must not contradict the file it points at.

On 2026-11-02 nineteen rows sat in that status. Eleven of them were
English-original episodes holding a single English track - Bob's Burgers x5,
It's Always Sunny x3, Futurama, and two films - so the flag was wrong in every
one. A flagged row is skipped by the picker forever, so those files were never
going to encode, and nothing said so: no log line, no failing check, no
dashboard warning. They surfaced only because the operator asked how things
were going.

The opposite direction already had a watcher (``no_done_with_foreign_audio``,
for a wrong-language file marked finished). These tests pin the missing half,
and in particular pin the two ways it must NOT fire:

  * a genuinely foreign-dub-only file stays flagged - that is the Amelie case
    the policy exists for, and calling it a violation would undo the policy;
  * an ``und`` track proves nothing, so a file waiting on whisper is never
    reported as a false flag.
"""

import sqlite3

import pytest

import tools.invariants as inv


def _entry(fp, original, langs, detected=None):
    """A media_report-shaped row. ``langs`` are track tags, ``detected`` optional."""
    detected = detected or [None] * len(langs)
    return {
        "filepath": fp,
        "tmdb": {"original_language": original} if original else {},
        "audio_streams": [{"language": lg, "detected_language": det} for lg, det in zip(langs, detected)],
    }


@pytest.fixture
def state_db(tmp_path, monkeypatch):
    """A state DB with the given paths in flagged_foreign_audio."""

    def install(flagged=(), other=()):
        db = tmp_path / "pipeline_state.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE pipeline_files (filepath TEXT PRIMARY KEY, status TEXT)")
        conn.executemany(
            "INSERT INTO pipeline_files VALUES (?, ?)",
            [(fp, "flagged_foreign_audio") for fp in flagged] + [(fp, "done") for fp in other],
        )
        conn.commit()
        conn.close()
        monkeypatch.setattr(inv, "_open_state_db", lambda: sqlite3.connect(db))
        return db

    return install


class TestFalseFlags:
    def test_english_original_with_english_track_is_a_violation(self, state_db):
        """The Bob's Burgers case: original=en, one eng track, still flagged."""
        state_db(flagged=["/m/Bobs S10E16.mkv"])
        report = {"files": [_entry("/m/Bobs S10E16.mkv", "en", ["eng"])]}
        res = inv.check_no_flagged_foreign_with_original_audio(report)
        assert not res.passed
        assert res.violations == ["/m/Bobs S10E16.mkv"]

    def test_message_says_they_will_never_encode(self, state_db):
        state_db(flagged=["/m/a.mkv"])
        res = inv.check_no_flagged_foreign_with_original_audio({"files": [_entry("/m/a.mkv", "en", ["eng"])]})
        assert "never encode" in res.message

    def test_detected_language_also_counts_as_proof(self, state_db):
        """An untagged track whisper identified as the original still disproves the flag."""
        state_db(flagged=["/m/b.mkv"])
        report = {"files": [_entry("/m/b.mkv", "ja", ["und"], detected=["jpn"])]}
        assert not inv.check_no_flagged_foreign_with_original_audio(report).passed

    def test_counts_every_offender(self, state_db):
        paths = [f"/m/ep{i}.mkv" for i in range(11)]
        state_db(flagged=paths)
        report = {"files": [_entry(p, "en", ["eng"]) for p in paths]}
        res = inv.check_no_flagged_foreign_with_original_audio(report)
        assert res.details["offenders"] == 11


class TestLegitimateFlags:
    def test_foreign_film_held_english_dub_only_stays_flagged(self, state_db):
        """The Amelie case. Reporting this would undo the original-language policy."""
        state_db(flagged=["/m/Two People Exchanging Saliva (2025).mkv"])
        report = {"files": [_entry("/m/Two People Exchanging Saliva (2025).mkv", "fr", ["eng"])]}
        res = inv.check_no_flagged_foreign_with_original_audio(report)
        assert res.passed
        assert res.details["evaluated"] == 1

    def test_und_track_proves_nothing(self, state_db):
        """A file waiting on whisper must never be called a false flag."""
        state_db(flagged=["/m/c.mkv"])
        report = {"files": [_entry("/m/c.mkv", "de", ["und"])]}
        assert inv.check_no_flagged_foreign_with_original_audio(report).passed

    def test_zxx_is_not_a_language(self, state_db):
        """Orchestral shorts are tagged zxx deliberately; it matches nothing."""
        state_db(flagged=["/m/d.mkv"])
        report = {"files": [_entry("/m/d.mkv", "en", ["zxx"])]}
        assert inv.check_no_flagged_foreign_with_original_audio(report).passed

    def test_missing_original_language_is_not_evaluated(self, state_db):
        """With no TMDb ground truth there is nothing to contradict the flag with."""
        state_db(flagged=["/m/Peter Pan (1953).mkv"])
        report = {"files": [_entry("/m/Peter Pan (1953).mkv", None, ["eng", "eng", "eng"])]}
        res = inv.check_no_flagged_foreign_with_original_audio(report)
        assert res.passed
        assert res.details["evaluated"] == 0

    def test_done_rows_are_ignored(self, state_db):
        """This invariant owns the flagged queue only; DONE has its own check."""
        state_db(flagged=[], other=["/m/e.mkv"])
        report = {"files": [_entry("/m/e.mkv", "en", ["eng"])]}
        res = inv.check_no_flagged_foreign_with_original_audio(report)
        assert res.passed
        assert res.details == {"flagged": 0}


class TestUnavailable:
    def test_no_report_is_skipped_not_failed(self, state_db, monkeypatch):
        state_db(flagged=["/m/f.mkv"])
        monkeypatch.setattr(inv, "_load_media_report", lambda: None)
        res = inv.check_no_flagged_foreign_with_original_audio()
        assert res.passed
        assert "skipped" in res.message

    def test_no_state_db_is_skipped_not_failed(self, monkeypatch):
        monkeypatch.setattr(inv, "_open_state_db", lambda: None)
        res = inv.check_no_flagged_foreign_with_original_audio({"files": []})
        assert res.passed
        assert "skipped" in res.message


class TestRegistration:
    def test_runs_as_part_of_the_suite(self):
        names = [getattr(fn, "__name__", "") for fn in inv._invariant_runners(skip_ssh=True)]
        assert "check_no_flagged_foreign_with_original_audio" in names
