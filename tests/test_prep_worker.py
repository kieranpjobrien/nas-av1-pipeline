"""Pass-1 tests for the prep-worker / encode-only split.

Goal: keep the GPU at 100% by moving CPU prep work (filename clean, language
detect with whisper, qualify gate, external sub scan, container remux) OUT
of the GPU worker thread and INTO a dedicated prep worker. Encode workers
short-circuit past prep when ``prep_done=True`` is cached on the state row.

These tests verify the contract — the actual whisper / ffmpeg calls are
mocked. Live behaviour is exercised by the integration suite + smoke runs.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipeline.control import PipelineControl
from pipeline.orchestrator import Orchestrator
from pipeline.state import FileStatus, PipelineState


@pytest.fixture(autouse=True)
def _mock_prep_external_calls(monkeypatch):
    """Autouse: stub the heavy I/O that the prep flow runs against the
    LOCAL fetched file. Tests use synthetic byte-blob files that
    aren't real videos, so the source-integrity probe (added
    2026-05-13 phase 3) and the local stream-strip helper would
    fail against them. We're not testing those layers here — the
    integration suite exercises real probes.
    """
    # Healthy probe stub
    from tools.probe_source_integrity import ProbeResult

    def _probe_healthy(filepath):
        return ProbeResult(
            filepath=filepath,
            duration_seconds=7200.0,
            healthy=True,
            probe_time_secs=0.1,
        )
    monkeypatch.setattr("tools.probe_source_integrity.probe_file", _probe_healthy)

    # Strip no-op stub — return the same path (no strip happened).
    # Updated 2026-05-13 21:30: the contract changed from
    # (ok, message) to (ok, stripped_path_or_message). The success
    # case now returns either local_path itself (no work) or a sibling
    # path. Tests using synthetic files don't strip, so return local_path.
    monkeypatch.setattr(
        "pipeline.prep_streams.strip_streams_locally",
        lambda path, item, config: (True, path),
    )


def _orch(tmp_path) -> Orchestrator:
    state = PipelineState(str(tmp_path / "state.db"))
    control = PipelineControl(str(tmp_path))
    config = {
        "gpu_concurrency": 1,
        "fetch_concurrency": 1,
        "prep_concurrency": 1,
        "queue_refresh_interval_secs": 0,
    }
    with patch("pipeline.orchestrator.signal") as fake_signal:
        fake_signal.SIGTERM = 0
        fake_signal.SIGINT = 0
        fake_signal.signal = MagicMock()
        return Orchestrator(config, state, str(tmp_path), control)


def _file_in_disk_state(tmp_path, fp_name: str) -> tuple[str, str]:
    """Create a fake fetched file on disk and return (filepath, local_path).

    filepath is the NAS-style logical path; local_path is the real on-disk
    fetch artefact the prep worker checks for.
    """
    nas_path = rf"\\NAS\Movies\{fp_name}"
    local_path = str(tmp_path / "fetch" / f"deadbeef_{fp_name}")
    (tmp_path / "fetch").mkdir(exist_ok=True)
    (tmp_path / "fetch" / f"deadbeef_{fp_name}").write_bytes(b"x")
    return nas_path, local_path


class TestPrepareForEncodeContract:
    """``prepare_for_encode`` produces prep_data and persists prep_done=True."""

    def test_returns_prep_data_and_marks_state(self, tmp_path):
        from pipeline.full_gamut import prepare_for_encode

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Test.mkv")
        item = {
            "filepath": nas_path,
            "filename": "Test.mkv",
            "library_type": "movie",
            "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
            "subtitle_streams": [],
            "tmdb": {"original_language": "en", "title": "Test"},
        }
        # Mark fetch done
        orch.state.set_file(nas_path, FileStatus.PROCESSING, local_path=local_path)

        # Mock the heavy lifting — we're not testing whisper/remux here, just
        # the function's contract: it produces prep_data and persists state.
        with patch("pipeline.full_gamut.detect_all_languages", side_effect=lambda i, **k: i), \
             patch("pipeline.filename.clean_filename", return_value=None), \
             patch("pipeline.full_gamut._find_external_subs", return_value=[]):
            from pipeline.qualify import QualifyOutcome, QualifyResult

            with patch("pipeline.qualify.qualify_file") as qfile:
                qfile.return_value = QualifyResult(
                    outcome=QualifyOutcome.QUALIFIED,
                    rationale="ready",
                    audio_keep_indices=[0],
                    sub_keep_indices=[],
                    detected_audio_languages={},
                    original_language="en",
                    enriched_entry=item,
                )
                prep_data = prepare_for_encode(nas_path, item, {}, orch.state, str(tmp_path))

        assert prep_data is not None
        assert prep_data["actual_input"] == local_path
        assert prep_data["output_path"].endswith(".mkv")
        assert prep_data["external_subs"] == []

        # State is marked prep_done=True with prep_data persisted.
        row = orch.state.get_file(nas_path)
        assert row["prep_done"] is True
        assert row["prep_data"]["actual_input"] == local_path
        assert row["stage"] == "prepped"

    def test_idempotent_on_prep_done(self, tmp_path):
        """Re-running prep on a prep_done=True file returns cached data without redoing work."""
        from pipeline.full_gamut import prepare_for_encode

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Cached.mkv")
        cached_prep = {
            "clean_name": "Cached.mkv",
            "actual_input": local_path,
            "remuxed_path": None,
            "external_subs": [],
            "output_path": str(tmp_path / "encoded" / "out.mkv"),
        }
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
            prep_done=True,
            prep_data=cached_prep,
        )

        # If prep ran the heavy work, this would error (no mocks installed).
        item = {"filepath": nas_path, "filename": "Cached.mkv"}
        result = prepare_for_encode(nas_path, item, {}, orch.state, str(tmp_path))
        assert result == cached_prep

    def test_concurrent_callers_serialise_via_per_filepath_lock(self, tmp_path):
        """Pin the 2026-05-14 06:21 Resident Alien S01E07 prep race.

        Pre-fix, a real prep worker and the GPU worker's
        ``_encode_only`` stale-prep fallback could both reach
        ``prepare_for_encode`` for the same filepath concurrently
        (the orchestrator's ``_pick_for_prep`` claim mechanism only
        covered prep-worker-vs-prep-worker, not GPU-fallback). Both
        called ``_mkvmerge_drop_streams_to_path`` against the same
        sibling target; the second got rc=2 and wrote ERROR over
        the first's success — observed live, status row was marked
        error with the 1.74 GB stripped file already sitting on
        disk from the winning worker.

        Post-fix: a per-filepath in-process Lock makes the function
        mutually-exclusive across all callers. The second worker
        blocks on entry, and by the time it acquires the lock the
        first has set ``prep_done=True`` so the top-of-function
        idempotence check returns the cached prep_data — no second
        mkvmerge call, no rc=2.

        We exercise this with two parallel threads that each call
        ``prepare_for_encode`` for the same filepath. Only the first
        thread should execute the qualify/strip work; the second
        should short-circuit on the cached prep_data the first wrote.
        """
        import threading
        from unittest.mock import patch

        from pipeline.full_gamut import prepare_for_encode

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Race.mkv")
        orch.state.set_file(nas_path, FileStatus.PROCESSING, local_path=local_path)

        item_template = {
            "filepath": nas_path,
            "filename": "Race.mkv",
            "library_type": "movie",
            "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
            "subtitle_streams": [],
            "tmdb": {"original_language": "en", "title": "Race"},
        }

        # Count how many times qualify_file actually runs. A correct
        # locking implementation runs it exactly once across the two
        # callers; without the lock, both threads pass the idempotence
        # check before either writes prep_done=True and qualify fires
        # twice.
        qualify_call_count = {"n": 0}
        qualify_call_count_lock = threading.Lock()

        def slow_qualify(item, config, **kw):
            from pipeline.qualify import QualifyOutcome, QualifyResult

            with qualify_call_count_lock:
                qualify_call_count["n"] += 1
            # Sleep so the second thread, if not blocked by the prep
            # lock, has plenty of time to also reach qualify.
            import time as _t
            _t.sleep(0.5)
            return QualifyResult(
                outcome=QualifyOutcome.QUALIFIED,
                rationale="ok",
                audio_keep_indices=[0],
                sub_keep_indices=[],
                detected_audio_languages={},
                original_language="en",
                enriched_entry=item,
            )

        results: list = [None, None]

        def call_prep(slot: int):
            with patch("pipeline.full_gamut.detect_all_languages", side_effect=lambda i, **k: i), \
                 patch("pipeline.filename.clean_filename", return_value=None), \
                 patch("pipeline.full_gamut._find_external_subs", return_value=[]), \
                 patch("pipeline.qualify.qualify_file", side_effect=slow_qualify):
                results[slot] = prepare_for_encode(
                    nas_path, dict(item_template), {}, orch.state, str(tmp_path)
                )

        t0 = threading.Thread(target=call_prep, args=(0,))
        t1 = threading.Thread(target=call_prep, args=(1,))
        t0.start()
        t1.start()
        t0.join(timeout=10)
        t1.join(timeout=10)

        assert not t0.is_alive(), "thread 0 deadlocked"
        assert not t1.is_alive(), "thread 1 deadlocked"

        # Both threads see the same prep_data — the winning one's.
        assert results[0] is not None
        assert results[1] is not None
        assert results[0] == results[1], (
            f"both threads must return the same cached prep_data; "
            f"got {results[0]!r} vs {results[1]!r}"
        )
        # Heavy work runs exactly once. The second thread short-circuits
        # on the idempotence check after the first releases the lock.
        assert qualify_call_count["n"] == 1, (
            f"qualify must run once total — concurrent callers should "
            f"serialise via the per-filepath lock and the second one "
            f"should short-circuit on prep_done=True. "
            f"Got {qualify_call_count['n']} calls."
        )

    def test_flagged_foreign_returns_none(self, tmp_path):
        """When qualify says FLAGGED_FOREIGN, prep_data is None and state shows the flag."""
        from pipeline.full_gamut import prepare_for_encode

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Bluey.mkv")
        item = {
            "filepath": nas_path,
            "filename": "Bluey.mkv",
            "library_type": "series",
            "audio_streams": [{"codec_raw": "eac3", "language": "swe"}],
            "subtitle_streams": [],
            "tmdb": {"original_language": "en", "title": "Bluey"},
        }
        orch.state.set_file(nas_path, FileStatus.PROCESSING, local_path=local_path)

        from pipeline.qualify import QualifyOutcome, QualifyResult

        flagged = QualifyResult(
            outcome=QualifyOutcome.FLAGGED_FOREIGN,
            rationale="audio is sv, original is en",
            audio_keep_indices=[],
            sub_keep_indices=[],
            detected_audio_languages={0: ("sv", 0.9, "whisper_tiny_3x30")},
            original_language="en",
            enriched_entry=item,
        )
        with patch("pipeline.full_gamut.detect_all_languages", side_effect=lambda i, **k: i), \
             patch("pipeline.filename.clean_filename", return_value=None), \
             patch("pipeline.qualify.qualify_file", return_value=flagged):
            result = prepare_for_encode(nas_path, item, {}, orch.state, str(tmp_path))

        assert result is None
        assert orch.state.get_file(nas_path)["status"] == FileStatus.FLAGGED_FOREIGN_AUDIO.value


class TestEncodeOnlyShortCircuit:
    """``full_gamut`` short-circuits past steps 1-5 when prep_data is cached."""

    def test_full_gamut_calls_encode_only_when_prep_done(self, tmp_path):
        from pipeline import full_gamut as fg

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Done.mkv")
        cached = {
            "clean_name": "Done.mkv",
            "actual_input": local_path,
            "remuxed_path": None,
            "external_subs": [],
            "output_path": str(tmp_path / "encoded" / "out.mkv"),
        }
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
            prep_done=True,
            prep_data=cached,
            detected_audio=[{"codec_raw": "eac3", "language": "eng"}],
            detected_subs=[],
        )

        item = {
            "filepath": nas_path,
            "filename": "Done.mkv",
            "library_type": "movie",
            "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
            "subtitle_streams": [],
        }

        called = {"encode_only": False, "prepare": False}

        def fake_encode_only(*a, **kw):
            called["encode_only"] = True
            return True

        def fake_prepare(*a, **kw):
            called["prepare"] = True
            return cached

        with patch("pipeline.full_gamut._encode_only", side_effect=fake_encode_only), \
             patch("pipeline.full_gamut.prepare_for_encode", side_effect=fake_prepare):
            ok = fg.full_gamut(nas_path, item, {}, orch.state, str(tmp_path))

        assert ok is True
        assert called["encode_only"] is True, "must short-circuit to _encode_only"
        assert called["prepare"] is False, "must NOT re-run prep when prep_done is cached"

    def test_full_gamut_routes_through_encode_only_even_without_prep_done(self, tmp_path):
        """Pin the 2026-05-14 Resident Alien S01E07 race fix.

        Pre-fix, when the GPU worker reached a file before the prep
        worker had a chance to run (most consistently: the very first
        file after every pipeline restart), ``full_gamut`` fell through
        to an inline STEP 1-5 block that built the ffmpeg command
        directly against the un-stripped fetched source. That inline
        block did NOT call ``strip_streams_locally`` — so the legacy
        ``_map_subtitle_streams`` selector saw the full source layout,
        ran title-based is_hi detection on every English track, and
        for files like Resident Alien S01E07 (only English sub titled
        ``İngilizce [CC]``) skipped the eng sub as HI/CC and instead
        mapped the source's forced Turkish sub. Output: foreign-sub
        PREP MISS on every cold-start file.

        Post-fix: ``full_gamut`` unconditionally delegates to
        ``_encode_only``, which has its own ``prep_data`` stale-guard
        + ``prepare_for_encode`` fallback. The strip step always runs.
        """
        from pipeline import full_gamut as fg

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "First-After-Restart.mkv")
        # Crucial setup: no prep_done flag, no prep_data — exactly the
        # state a freshly-fetched row sits in when the GPU worker beats
        # the prep worker to it.
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
        )

        item = {
            "filepath": nas_path,
            "filename": "First-After-Restart.mkv",
            "library_type": "series",
            "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
            "subtitle_streams": [
                # The exact S01E07 shape: only-English sub with a
                # CC-titled-in-Turkish track, plus a forced foreign sub.
                {"codec": "subrip", "language": "eng", "title": "İngilizce [CC]"},
                {"codec": "subrip", "language": "tur",
                 "title": "Turkish [ForcedNarrative]"},
            ],
            "tmdb": {"original_language": "en"},
        }

        called = {"encode_only": False}

        def fake_encode_only(*a, **kw):
            called["encode_only"] = True
            return True

        with patch("pipeline.full_gamut._encode_only", side_effect=fake_encode_only):
            ok = fg.full_gamut(nas_path, item, {}, orch.state, str(tmp_path))

        assert ok is True
        assert called["encode_only"] is True, (
            "even without prep_done cached, full_gamut MUST go through "
            "_encode_only — the inline encode path skipped strip and "
            "raced the prep worker. Routing everything through "
            "_encode_only is what guarantees the encoder consumes a "
            "stripped input."
        )


class TestEncodeOnlyStaleExistingSnapshot:
    """Pin the 2026-05-14 15:15 The Favourite (2018) restore-bug fix.

    ``_encode_only`` reads ``existing = state.get_file(filepath)`` at
    the top, then guards against stale prep_data and re-runs
    ``prepare_for_encode`` inline if needed. The fresh prepare_for_encode
    call mutates ``item.audio_streams`` / ``item.subtitle_streams`` to
    the post-strip layout AND persists fresh ``detected_audio`` /
    ``detected_subs`` to state. But the restore block at the bottom
    used the ORIGINAL ``existing`` snapshot — captured BEFORE inline
    prep ran — so its detected_audio/subs were stale.

    Result: prep correctly stripped a 2-audio source to 1 audio, the
    re-probe updated item.audio_streams to a 1-element list, but the
    restore line then overrode it BACK to the stale 2-element list.
    The encoder's ``_select_audio_streams`` returned ``[1]`` (keep
    index 1 of the 2-element view), and ``build_ffmpeg_cmd`` emitted
    ``-map 0:a:1`` against the 1-audio stripped input. ffmpeg refused
    with "Stream map '' matches no streams".

    Post-fix: after inline prep runs, ``existing`` is re-read from
    state so the restore picks up the fresh detected_audio/subs.
    """

    def test_inline_prep_fallback_uses_fresh_state_for_restore(self, tmp_path):
        from unittest.mock import patch

        from pipeline.full_gamut import _encode_only

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Favourite.mkv")

        # Initial state: prep_data points at a stripped sibling that does NOT
        # exist on disk (the stale-prep guard will trip). detected_audio
        # holds the OLD pre-strip 2-element list. The inline-prep fallback
        # must update detected_audio to the post-strip 1-element view before
        # the restore reads it.
        stale_stripped = str(tmp_path / "fetch" / "missing.stripped.mkv")
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
            prep_done=True,
            prep_data={
                "clean_name": "Favourite.mkv",
                "actual_input": stale_stripped,
                "remuxed_path": None,
                "external_subs": [],
                "output_path": str(tmp_path / "encoded" / "out.mkv"),
            },
            detected_audio=[
                # Stale pre-strip 2-element list — the test's tripwire.
                {"codec_raw": "eac3", "language": "und", "title": ""},
                {"codec_raw": "eac3", "language": "eng", "title": ""},
            ],
            detected_subs=[],
        )

        item = {
            "filepath": nas_path,
            "filename": "Favourite.mkv",
            "library_type": "movie",
            "audio_streams": [
                {"codec_raw": "eac3", "language": "und", "title": ""},
                {"codec_raw": "eac3", "language": "eng", "title": ""},
            ],
            "subtitle_streams": [],
            "tmdb": {"original_language": "en", "title": "Favourite"},
        }

        # Fresh-prep result that the inline fallback would produce — points
        # at a stripped sibling that exists, with post-strip 1-element audio.
        new_stripped = str(tmp_path / "fetch" / "deadbeef_Favourite.mkv.stripped.mkv")
        (tmp_path / "fetch" / "deadbeef_Favourite.mkv.stripped.mkv").write_bytes(b"x")
        fresh_prep = {
            "clean_name": "Favourite.mkv",
            "actual_input": new_stripped,
            "remuxed_path": None,
            "external_subs": [],
            "output_path": str(tmp_path / "encoded" / "out.mkv"),
        }
        # Post-strip audio view that the fresh prep would persist.
        post_strip_audio = [
            {"codec": "EAC3", "codec_raw": "eac3", "language": "eng",
             "title": "", "channels": 6},
        ]

        def fake_prepare(fp, itm, cfg, st, staging):
            # Mirror what real prepare_for_encode does on the fresh path:
            # mutate item AND persist to state.
            itm["audio_streams"] = list(post_strip_audio)
            st.set_file(
                fp,
                FileStatus.PROCESSING,
                stage="prepped",
                prep_done=True,
                prep_data=fresh_prep,
                detected_audio=post_strip_audio,
                detected_subs=[],
            )
            return fresh_prep

        # Capture item.audio_streams at the point the encoder is about to
        # build its command — that's what determines whether the restore
        # picked up the fresh post-strip view or the stale pre-strip view.
        observed_audio: list = []

        def fake_run_encode(*a, **kw):
            # _encode_only calls _run_encode(cmd, actual_input, output_path,
            # item, ...) — item is positional arg 3.
            observed_audio.append(list(a[3].get("audio_streams", [])))
            # Don't bother building a real output; signal success.
            output_path = a[2]
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "wb") as fh:
                fh.write(b"x" * 1000)
            return True

        fake_params = {"cq": 27, "base_cq": 27, "cq_offset": 0, "preset": "p5", "content_grade": "default"}
        with patch("pipeline.full_gamut.prepare_for_encode", side_effect=fake_prepare), \
             patch("pipeline.full_gamut._run_encode", side_effect=fake_run_encode), \
             patch("pipeline.full_gamut.build_ffmpeg_cmd",
                   return_value=["ffmpeg", "-i", "stub"]), \
             patch("pipeline.full_gamut.resolve_encode_params", return_value=fake_params), \
             patch("pipeline.full_gamut.get_res_key", return_value="1080p"):
            _encode_only(nas_path, item, {}, orch.state, str(tmp_path), gpu_semaphore=None)

        assert observed_audio, "_run_encode was never called"
        seen = observed_audio[0]
        assert len(seen) == 1, (
            f"after inline-prep fallback, item.audio_streams should be the "
            f"post-strip 1-element list — got {len(seen)} elements: {seen!r}. "
            f"Stale `existing` snapshot is overriding the fresh prep mutation."
        )


class TestPrepWorkerPicker:
    """Prep worker picks fetched-but-not-prepped files via _pick_for_prep."""

    def test_picks_fetched_unpprepped_file(self, tmp_path):
        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "A.mkv")
        item = {"filepath": nas_path, "filename": "A.mkv", "file_size_bytes": 100}

        orch.state.set_file(nas_path, FileStatus.PROCESSING, local_path=local_path)
        # No prep_done flag → picker should pick it.
        picked = orch._pick_for_prep([item])
        assert picked is not None
        assert picked["filepath"] == nas_path
        # And it must claim the slot so a sibling prep worker doesn't double-pick.
        assert nas_path in orch._prepping

    def test_skips_already_prepped(self, tmp_path):
        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "B.mkv")
        item = {"filepath": nas_path, "filename": "B.mkv", "file_size_bytes": 100}

        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
            prep_done=True,
            prep_data={"actual_input": local_path},
        )
        assert orch._pick_for_prep([item]) is None

    def test_skips_unfetched(self, tmp_path):
        """File still PENDING (not fetched) → prep worker doesn't touch it."""
        orch = _orch(tmp_path)
        item = {
            "filepath": r"\\NAS\Movies\NotFetched.mkv",
            "filename": "NotFetched.mkv",
            "file_size_bytes": 100,
        }
        orch.state.set_file(item["filepath"], FileStatus.PENDING)
        assert orch._pick_for_prep([item]) is None

    def test_release_clears_prepping_set(self, tmp_path):
        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "C.mkv")
        item = {"filepath": nas_path, "filename": "C.mkv", "file_size_bytes": 100}
        orch.state.set_file(nas_path, FileStatus.PROCESSING, local_path=local_path)

        orch._pick_for_prep([item])
        assert nas_path in orch._prepping
        orch._release_prep(nas_path)
        assert nas_path not in orch._prepping


class TestUploadWorkerPicker:
    """Upload worker picks rows in UPLOADING status."""

    def test_pick_for_upload_returns_uploading_row(self, tmp_path):
        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\Encoded.mkv"
        orch.state.set_file(fp, FileStatus.UPLOADING, output_path=str(tmp_path / "out.mkv"))
        picked = orch._pick_for_upload()
        assert picked == fp
        # Slot is claimed so a sibling upload worker doesn't double-pick.
        assert fp in orch._uploading

    def test_pick_for_upload_skips_non_uploading(self, tmp_path):
        orch = _orch(tmp_path)
        # File in PROCESSING should NOT be picked
        fp = r"\\NAS\Movies\Processing.mkv"
        orch.state.set_file(fp, FileStatus.PROCESSING)
        assert orch._pick_for_upload() is None

    def test_pick_for_upload_skips_already_claimed(self, tmp_path):
        """A file already in _uploading is owned by another worker; skip."""
        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\Owned.mkv"
        orch.state.set_file(fp, FileStatus.UPLOADING)
        orch._uploading.add(fp)
        assert orch._pick_for_upload() is None

    def test_release_upload_clears_set(self, tmp_path):
        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\Released.mkv"
        orch.state.set_file(fp, FileStatus.UPLOADING)
        orch._pick_for_upload()
        assert fp in orch._uploading
        orch._release_upload(fp)
        assert fp not in orch._uploading


class TestGpuWorkerHandsOffToUploadWorker:
    """When upload_concurrency >= 1, the GPU worker does NOT call finalize_upload inline."""

    def test_finalize_upload_not_called_when_upload_worker_present(self, tmp_path, monkeypatch):
        from pipeline.control import PipelineControl
        from pipeline.state import PipelineState

        state = PipelineState(str(tmp_path / "state.db"))
        control = PipelineControl(str(tmp_path))
        config = {
            "gpu_concurrency": 1,
            "fetch_concurrency": 1,
            "prep_concurrency": 0,
            "upload_concurrency": 1,  # decoupled upload
            "queue_refresh_interval_secs": 0,
        }
        with patch("pipeline.orchestrator.signal") as fake_signal:
            fake_signal.SIGTERM = 0
            fake_signal.SIGINT = 0
            fake_signal.signal = MagicMock()
            orch = Orchestrator(config, state, str(tmp_path), control)

        item = {
            "filepath": str(tmp_path / "x.mkv"),
            "filename": "x.mkv",
            "file_size_bytes": 100,
            "video": {},
        }
        (tmp_path / "x.mkv").write_bytes(b"x")
        queue = [item]

        finalize_called: list[bool] = []

        def fake_full_gamut(fp, it, cfg, st, staging, *, gpu_semaphore=None):
            return True

        def fake_finalize_upload(*a, **kw):
            finalize_called.append(True)
            return True

        monkeypatch.setattr("pipeline.orchestrator.full_gamut", fake_full_gamut)
        monkeypatch.setattr("pipeline.orchestrator.finalize_upload", fake_finalize_upload)

        import threading
        import time

        def stop_soon():
            time.sleep(0.3)
            orch._shutdown.set()

        threading.Thread(target=stop_soon, daemon=True).start()
        orch._gpu_worker(queue, [], worker_id=0)

        assert finalize_called == [], (
            "GPU worker called finalize_upload inline; it must hand off to upload worker"
        )

    def test_finalize_upload_inline_when_upload_concurrency_zero(self, tmp_path, monkeypatch):
        """upload_concurrency=0 restores the legacy inline-upload behaviour for tests."""
        from pipeline.control import PipelineControl
        from pipeline.state import PipelineState

        state = PipelineState(str(tmp_path / "state.db"))
        control = PipelineControl(str(tmp_path))
        config = {
            "gpu_concurrency": 1,
            "fetch_concurrency": 1,
            "prep_concurrency": 0,
            "upload_concurrency": 0,  # inline mode
            "queue_refresh_interval_secs": 0,
        }
        with patch("pipeline.orchestrator.signal") as fake_signal:
            fake_signal.SIGTERM = 0
            fake_signal.SIGINT = 0
            fake_signal.signal = MagicMock()
            orch = Orchestrator(config, state, str(tmp_path), control)

        item = {
            "filepath": str(tmp_path / "y.mkv"),
            "filename": "y.mkv",
            "file_size_bytes": 100,
            "video": {},
        }
        (tmp_path / "y.mkv").write_bytes(b"y")
        queue = [item]

        finalize_called: list[bool] = []
        monkeypatch.setattr(
            "pipeline.orchestrator.full_gamut",
            lambda *a, **kw: True,
        )
        monkeypatch.setattr(
            "pipeline.orchestrator.finalize_upload",
            lambda *a, **kw: finalize_called.append(True) or True,
        )

        import threading
        import time

        def stop_soon():
            time.sleep(0.3)
            orch._shutdown.set()

        threading.Thread(target=stop_soon, daemon=True).start()
        orch._gpu_worker(queue, [], worker_id=0)

        assert finalize_called, (
            "with upload_concurrency=0 the GPU worker should still call finalize_upload inline"
        )


class TestGapFillerDrainAndRescan:
    """Gap filler now loops over the queue rather than draining once and exiting.

    Lets new files added mid-session by the queue refresh worker get picked up
    without a pipeline restart.
    """

    def test_gap_filler_worker_loops_until_shutdown(self, tmp_path, monkeypatch):
        from pipeline.control import PipelineControl
        from pipeline.state import PipelineState

        state = PipelineState(str(tmp_path / "state.db"))
        control = PipelineControl(str(tmp_path))
        config = {
            "gpu_concurrency": 1,
            "fetch_concurrency": 1,
            "prep_concurrency": 0,
            "upload_concurrency": 0,
            "queue_refresh_interval_secs": 0,
            # Tight rescan interval so the test doesn't have to wait long.
            "gap_filler_rescan_interval_secs": 0.1,
        }
        with patch("pipeline.orchestrator.signal") as fake_signal:
            fake_signal.SIGTERM = 0
            fake_signal.SIGINT = 0
            fake_signal.signal = MagicMock()
            orch = Orchestrator(config, state, str(tmp_path), control)

        # Replace the heavy work with a counter so the test runs fast.
        passes: list[int] = []

        def fake_pass(self_, queue, pass_num):
            passes.append(pass_num)
            return 0  # nothing processed → idle pause = rescan_interval

        monkeypatch.setattr(Orchestrator, "_gap_filler_pass", fake_pass)

        import threading
        import time

        def stop_soon():
            time.sleep(0.5)
            orch._shutdown.set()

        threading.Thread(target=stop_soon, daemon=True).start()
        orch._gap_filler_worker([])

        assert len(passes) >= 2, (
            f"gap filler must rescan after a drain (got {len(passes)} passes); "
            "without the loop, new files added mid-session never get picked up"
        )
        # passes are monotonically increasing pass numbers
        assert passes == list(range(1, len(passes) + 1))


class TestEncodeOnlyStalePrepGuard:
    """Regression for The Lost Thing 2026-04-29 incident.

    Repro: the prep worker successfully prepped the file, cached prep_data
    in state, encode + upload reached UPLOADING. Pipeline restarted before
    DONE landed. On startup the orchestrator cleans F:/AV1_Staging/fetch/
    of orphans. Reset_non_terminal flips UPLOADING → PENDING. The fresh
    queue picks the file up, sees prep_done=True with cached prep_data
    pointing at the now-deleted local file, fires _encode_only — ffmpeg
    immediately fails with ENOENT.

    The dashboard then displays the file as "encoding 12% stale 11h 50m"
    because the encode-failed state-write got race-overwritten by the
    fetch worker's later progress update.

    Fix: _encode_only must verify the on-disk fetch+remux files still
    exist before trusting cached prep_data. If gone, treat as
    invalidated and fall back to inline prep.
    """

    def test_stale_prep_data_falls_back_to_inline_prep(self, tmp_path):
        """When prep_data points at a missing local file, drop it + re-prep."""
        from pipeline.full_gamut import _encode_only

        orch = _orch(tmp_path)
        nas_path = r"\\NAS\Movies\Stale.mkv"
        # Local path is recorded in state but the file no longer exists on disk
        # (orchestrator startup cleaned F:/AV1_Staging/fetch/).
        ghost_local = str(tmp_path / "fetch" / "deadbeef_Stale.mkv")
        ghost_remux = ghost_local + ".remux.mkv"
        # Note: NEITHER ghost_local nor ghost_remux is created on disk.

        cached_prep = {
            "clean_name": "Stale.mkv",
            "actual_input": ghost_remux,  # <-- the smoking gun: cached but missing
            "remuxed_path": ghost_remux,
            "external_subs": [],
            "output_path": str(tmp_path / "encoded" / "out.mkv"),
        }
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=ghost_local,
            prep_done=True,
            prep_data=cached_prep,
        )

        # Patch prepare_for_encode so the fallback path is observable but
        # doesn't actually run heavy work. Returning None signals "couldn't
        # prep" — that's enough to prove the guard kicked in and routed to
        # the fallback rather than blindly using cached_prep.
        from unittest.mock import patch as _patch
        with _patch("pipeline.full_gamut.prepare_for_encode", return_value=None) as m:
            item = {"filepath": nas_path, "filename": "Stale.mkv"}
            ok = _encode_only(nas_path, item, {}, orch.state, str(tmp_path))

        assert m.called, (
            "_encode_only must fall back to prepare_for_encode when prep_data "
            "points at a missing input file"
        )
        # prep_result=None → _encode_only returns False (the proper failure path,
        # not a silent ENOENT-on-ffmpeg).
        assert ok is False

    def test_fresh_prep_data_skips_inline_prep(self, tmp_path):
        """Sanity: when prep_data is valid (files exist), no fallback fires."""
        from pipeline.full_gamut import _encode_only

        orch = _orch(tmp_path)
        nas_path, local_path = _file_in_disk_state(tmp_path, "Fresh.mkv")
        # The remuxed_path also needs to exist (or actual_input == local_path).
        cached_prep = {
            "clean_name": "Fresh.mkv",
            "actual_input": local_path,  # exists
            "remuxed_path": None,
            "external_subs": [],
            "output_path": str(tmp_path / "encoded" / "out.mkv"),
        }
        orch.state.set_file(
            nas_path,
            FileStatus.PROCESSING,
            local_path=local_path,
            prep_done=True,
            prep_data=cached_prep,
        )

        # _encode_only would call _run_encode after the prep check; mock it to
        # fail fast so we don't actually invoke ffmpeg. The point of the test
        # is the prep-guard branch, not the encode itself.
        from unittest.mock import patch as _patch
        with _patch("pipeline.full_gamut.prepare_for_encode") as m_prep, \
             _patch("pipeline.full_gamut._run_encode", return_value=False):
            item = {"filepath": nas_path, "filename": "Fresh.mkv"}
            _encode_only(nas_path, item, {}, orch.state, str(tmp_path))

        assert not m_prep.called, (
            "When prep_data is valid (file exists), _encode_only must NOT "
            "fall back to prepare_for_encode"
        )
