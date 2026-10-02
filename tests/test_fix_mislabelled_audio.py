"""Deciding by ear, safely.

The Bluey releases tag all three audio tracks 'eng', two of them are Mandarin,
and the Mandarin one holds the default flag. Everything here exists to make
sure the fix cannot do more damage than the bug.
"""

import pytest

import tools.fix_mislabelled_audio as fx


def fake_model():
    return object()


def build_plan(monkeypatch, tracks, heard, original_language="en"):
    """tracks: list of (tag, name, is_default); heard: list of (lang, conf)."""
    monkeypatch.setattr(
        fx,
        "identify",
        lambda _p: {
            "tracks": [
                {
                    "type": "audio",
                    "id": i,
                    "properties": {"language": t[0], "track_name": t[1], "default_track": t[2]},
                }
                for i, t in enumerate(tracks)
            ]
        },
    )
    monkeypatch.setattr(fx, "duration_secs", lambda _p: 1300.0)
    monkeypatch.setattr(fx, "detect", lambda _p, i, _m, _d: heard[i])
    return fx.plan("X.mkv", original_language, fake_model())


class TestTheBlueyCase:
    """S03E15 Explorers, verbatim from the probe."""

    TRACKS = [("eng", "Taiwan", True), ("eng", "", False), ("eng", "", False)]
    HEARD = [("zh", 1.00), ("en", 0.88), ("zh", 0.81)]

    def test_both_chinese_tracks_are_dropped(self, monkeypatch):
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert p["drop"] == [0, 2]

    def test_the_english_track_is_kept(self, monkeypatch):
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert p["keep"] == [1]

    def test_the_english_track_becomes_default(self, monkeypatch):
        """The whole user-visible bug: a Chinese track held the default flag."""
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert p["default_index"] == 1

    def test_the_lying_tag_is_ignored_entirely(self, monkeypatch):
        """All three claim 'eng'. The tag must carry no weight."""
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert all(d["tagged"] == "eng" for d in p["tracks"])
        assert p["drop"] == [0, 2]


class TestNeverStripOnWeakEvidence:
    def test_low_confidence_foreign_is_kept(self, monkeypatch):
        """Inviolate: never strip a track without knowing its language."""
        p = build_plan(
            monkeypatch,
            [("eng", "", True), ("eng", "", False)],
            [("en", 0.95), ("zh", 0.40)],
        )
        assert p["drop"] == []

    def test_undetectable_track_is_kept(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("eng", "", True), ("und", "", False)],
            [("en", 0.95), (None, 0.0)],
        )
        assert p["drop"] == []

    def test_confidence_exactly_at_the_floor_strips(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("eng", "", True), ("eng", "", False)],
            [("en", 0.95), ("zh", fx.MIN_CONFIDENCE)],
        )
        assert p["drop"] == [1]


class TestNeverZeroAudio:
    def test_all_foreign_is_refused_whole(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("eng", "Taiwan", True), ("eng", "", False)],
            [("zh", 0.99), ("zh", 0.98)],
        )
        assert p["keep"] == []
        assert "no audio" in p["skip"]

    def test_a_single_survivor_is_enough(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("eng", "Taiwan", True), ("eng", "", False)],
            [("zh", 0.99), ("en", 0.75)],
        )
        assert p["keep"] == [1] and p["skip"] == ""


class TestNeverPromoteAnUnidentifiedTrack:
    """Bluey S03E18 Rain detects as br(0.27)/br(0.32)/br(0.35) - the samples
    land on music. The first keeper there is the TAIWANESE track, so a
    'default to the first keeper' fallback recreates the original bug by hand."""

    TRACKS = [("eng", "Taiwan", True), ("eng", "", False), ("eng", "", False)]
    HEARD = [("br", 0.27), ("br", 0.32), ("br", 0.35)]

    def test_nothing_is_dropped(self, monkeypatch):
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert p["drop"] == []

    def test_no_default_is_chosen(self, monkeypatch):
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert p["default_index"] is None

    def test_it_says_why_rather_than_silently_doing_nothing(self, monkeypatch):
        p = build_plan(monkeypatch, self.TRACKS, self.HEARD)
        assert "confidence" in p["skip"]

    def test_a_weak_english_reading_cannot_win_the_default(self, monkeypatch):
        """Below the floor is below the floor, even if it is the only English."""
        p = build_plan(
            monkeypatch,
            [("eng", "Taiwan", True), ("eng", "", False)],
            [("zh", 0.99), ("en", 0.40)],
        )
        assert p["drop"] == [0]
        assert p["default_index"] is None


class TestDualAudioPolicy:
    """Animation keeps BOTH the original language and English (2026-07-11)."""

    def test_japanese_original_keeps_japanese_and_english(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("jpn", "", True), ("eng", "", False), ("fre", "", False)],
            [("ja", 0.99), ("en", 0.97), ("fr", 0.96)],
            original_language="ja",
        )
        assert set(p["keep"]) == {0, 1}
        assert p["drop"] == [2]

    def test_default_goes_to_the_original_language_not_english(self, monkeypatch):
        p = build_plan(
            monkeypatch,
            [("eng", "", True), ("jpn", "", False)],
            [("en", 0.97), ("ja", 0.99)],
            original_language="ja",
        )
        assert p["default_index"] == 1


class TestMajorityVoting:
    """A single 20s window can land on music or a silent beat - that is what
    gave the real English track only 0.35 under the old single-sample tool."""

    def test_unanimous_beats_a_lone_high_score(self):
        assert len(fx.SAMPLE_POINTS) >= 3

    def test_minority_verdicts_are_discounted(self, monkeypatch):
        calls = iter([("zh", 0.9), ("en", 0.9), ("en", 0.9)])

        class M:
            def transcribe(self, *_a, **_k):
                lang, prob = next(calls)
                return [], type("I", (), {"language": lang, "language_probability": prob})()

        monkeypatch.setattr(fx.subprocess, "run", lambda *_a, **_k: type("R", (), {"returncode": 0})())
        monkeypatch.setattr(fx.os.path, "exists", lambda _p: True)
        lang, conf = fx.detect("X.mkv", 0, M(), 1300.0)
        assert lang == "en"
        # 2 of 3 samples agreeing scales the score down, not up
        assert conf == pytest.approx(0.9 * (2 / 3))
