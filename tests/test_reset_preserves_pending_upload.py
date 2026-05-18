"""Pin the 2026-05-19 reset-preserves-uploadable carve-out.

Pre-2026-05-19, ``state.reset_non_terminal`` flattened every non-terminal
row to PENDING and cleared ``output_path``. That's correct for crash
residue (genuine in-flight workers that died), but wrong for
``UPLOADING`` rows with ``stage="pending_upload"`` AND an
``output_path`` that still exists on disk — those are finished encodes
waiting on a dead upload worker; the encode artefact is real and
usable. Resetting them forces a full re-encode of work already on
disk.

The canonical case from 2026-05-19: pipeline GPU worker died on a
fetch failure at 21:49 the night before, the upload worker had
already stopped picking work some time earlier. 23 encoded outputs
(57 GB) sat in ``F:/AV1_Staging/encoded/`` for 6-8 hours with state
rows in ``uploading/pending_upload``. A naive restart would wipe
``output_path`` from all 23 → re-encode the lot.

Post-fix: ``reset_non_terminal`` skips a row when:
  * status == UPLOADING
  * stage == "pending_upload"
  * output_path is set
  * the output file actually exists on disk

All four conditions must hold. Anything else (e.g. UPLOADING with
no output_path on disk, or a different stage) is still treated as
crash residue and gets reset.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.state import FileStatus, PipelineState


def _state(tmp_path: Path) -> PipelineState:
    return PipelineState(str(tmp_path / "state.db"))


def test_uploading_pending_upload_with_real_output_preserved(tmp_path):
    """The exact 2026-05-19 case: status=UPLOADING, stage=pending_upload,
    output_path exists on disk → must NOT be reset."""
    st = _state(tmp_path)
    nas_path = r"\\NAS\Movies\Test.mkv"
    encoded = tmp_path / "encoded" / "deadbeef_Test.mkv"
    encoded.parent.mkdir()
    encoded.write_bytes(b"av1bytes" * 100)

    st.set_file(
        nas_path,
        FileStatus.UPLOADING,
        stage="pending_upload",
        output_path=str(encoded),
        encode_time_secs=120,
    )

    reset_count = st.reset_non_terminal()
    assert reset_count == 0, "uploadable row must NOT be counted as reset"

    row = st.get_file(nas_path)
    assert row["status"] == FileStatus.UPLOADING.value
    assert row["stage"] == "pending_upload"
    assert row["output_path"] == str(encoded), "output_path must survive — the upload worker needs it"


def test_uploading_pending_upload_with_missing_output_still_reset(tmp_path):
    """If the encoded output was cleaned (e.g. staging dir purge), the
    pending_upload state is bogus — that file IS crash residue. Reset
    so the file re-enters the queue from pending."""
    st = _state(tmp_path)
    nas_path = r"\\NAS\Movies\Cleaned.mkv"

    st.set_file(
        nas_path,
        FileStatus.UPLOADING,
        stage="pending_upload",
        output_path=str(tmp_path / "encoded" / "missing_Cleaned.mkv"),  # doesn't exist
    )

    reset_count = st.reset_non_terminal()
    assert reset_count == 1

    row = st.get_file(nas_path)
    assert row["status"] == FileStatus.PENDING.value
    assert not row.get("output_path"), "output_path must be cleared on reset"


def test_uploading_with_different_stage_still_reset(tmp_path):
    """An UPLOADING row at any other stage (e.g. 'verify' or None) is
    crash residue from an incomplete upload — reset normally."""
    st = _state(tmp_path)
    nas_path = r"\\NAS\Movies\InFlight.mkv"
    encoded = tmp_path / "encoded" / "inflight.mkv"
    encoded.parent.mkdir()
    encoded.write_bytes(b"x")

    st.set_file(
        nas_path,
        FileStatus.UPLOADING,
        stage="verify",  # not pending_upload
        output_path=str(encoded),
    )

    reset_count = st.reset_non_terminal()
    assert reset_count == 1

    row = st.get_file(nas_path)
    assert row["status"] == FileStatus.PENDING.value


def test_processing_status_always_reset(tmp_path):
    """Processing rows are always crash residue. Even if output_path
    happens to be set, reset them."""
    st = _state(tmp_path)
    nas_path = r"\\NAS\Movies\MidEncode.mkv"
    encoded = tmp_path / "encoded" / "mid.mkv"
    encoded.parent.mkdir()
    encoded.write_bytes(b"x")

    st.set_file(
        nas_path,
        FileStatus.PROCESSING,
        stage="encoding",
        output_path=str(encoded),
    )

    reset_count = st.reset_non_terminal()
    assert reset_count == 1

    row = st.get_file(nas_path)
    assert row["status"] == FileStatus.PENDING.value


def test_done_and_flagged_never_reset(tmp_path):
    """Terminal statuses are never touched."""
    st = _state(tmp_path)
    for fp, status in [
        (r"\\NAS\done.mkv", FileStatus.DONE),
        (r"\\NAS\corrupt.mkv", FileStatus.FLAGGED_CORRUPT),
        (r"\\NAS\foreign.mkv", FileStatus.FLAGGED_FOREIGN_AUDIO),
    ]:
        st.set_file(fp, status, mode="full_gamut")

    reset_count = st.reset_non_terminal()
    assert reset_count == 0
    for fp, status in [
        (r"\\NAS\done.mkv", FileStatus.DONE),
        (r"\\NAS\corrupt.mkv", FileStatus.FLAGGED_CORRUPT),
        (r"\\NAS\foreign.mkv", FileStatus.FLAGGED_FOREIGN_AUDIO),
    ]:
        assert st.get_file(fp)["status"] == status.value


def test_mixed_batch_preserves_uploadable_resets_others(tmp_path):
    """Drive the full case: 1 uploadable + 2 crash-residue rows. Only
    the crash residue gets reset."""
    st = _state(tmp_path)

    # Uploadable
    fp_up = r"\\NAS\uploadable.mkv"
    enc = tmp_path / "encoded" / "u.mkv"
    enc.parent.mkdir()
    enc.write_bytes(b"x")
    st.set_file(fp_up, FileStatus.UPLOADING, stage="pending_upload", output_path=str(enc))

    # Crash residue: stuck mid-encode
    fp_proc = r"\\NAS\stuck_encoding.mkv"
    st.set_file(fp_proc, FileStatus.PROCESSING, stage="encoding")

    # Crash residue: stuck uploading without an encoded output
    fp_lost = r"\\NAS\stuck_uploading_no_output.mkv"
    st.set_file(fp_lost, FileStatus.UPLOADING, stage="pending_upload",
                output_path=str(tmp_path / "encoded" / "ghost.mkv"))

    reset_count = st.reset_non_terminal()
    assert reset_count == 2

    assert st.get_file(fp_up)["status"] == FileStatus.UPLOADING.value
    assert st.get_file(fp_up)["output_path"] == str(enc)
    assert st.get_file(fp_proc)["status"] == FileStatus.PENDING.value
    assert st.get_file(fp_lost)["status"] == FileStatus.PENDING.value
