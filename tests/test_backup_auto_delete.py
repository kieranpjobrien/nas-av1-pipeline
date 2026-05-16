"""Pin the 2026-05-16 auto-delete-backup policy.

Pre-2026-05-16, ``finalize_upload`` left the ``.original.bak`` file in
place forever — comment said "DO NOT auto-delete .original.bak, Synology
#recycle captures a safety copy". Result: 631 backups totalling 3.62 TB
sat on NAS before the user asked for the auto-cleanup ("get the garbage
old files fucked the fuck off").

Post-2026-05-16: once the new file passes all post-replace verification
(compliance gate, filename standards, TMDb tag stamp, sidecar cleanup),
the ``.original.bak`` is deleted just before the DONE state transition.
A backup-removal failure does NOT fail the encode — the new file is
already in place and that's what matters; a stuck bak is just leftover
the next bulk purge picks up.

These tests pin the structural placement (introspection only — the
flow has too many SMB-touching steps to mock cleanly in a unit test).
"""

from __future__ import annotations

import inspect


def test_finalize_upload_deletes_backup_after_replace():
    """The auto-delete block exists and lives inside finalize_upload."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg.finalize_upload)
    # The deletion lives inside an existence guard so an absent bak
    # (fresh-encode case) is a no-op.
    assert "if os.path.exists(backup_path):" in src
    # And actually removes the file.
    assert "os.remove(backup_path)" in src, (
        "the deletion must call os.remove on the .original.bak path. "
        "Anything softer (rename / move to /tmp) defeats the user's "
        "explicit 'delete the shite' policy."
    )


def test_backup_deletion_runs_after_all_post_replace_verification():
    """Placement: the delete must come AFTER the sidecar cleanup block
    (the last post-replace verification step) and BEFORE the DONE
    state.set_file transition. If it ran earlier, an early-return from
    the filename-standards or TMDb stages would leave the new file in
    place without the bak as a rollback target."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg.finalize_upload)
    sidecar_marker = "Sidecar cleanup: removed"
    delete_marker = "os.remove(backup_path)"
    done_marker = "FileStatus.DONE"

    sidecar_idx = src.find(sidecar_marker)
    delete_idx = src.find(delete_marker)
    done_idx = src.find(done_marker)

    assert sidecar_idx >= 0, "sidecar cleanup block not found — check the marker"
    assert delete_idx >= 0, "backup-delete block not found"
    assert done_idx >= 0, "DONE state transition not found"

    assert sidecar_idx < delete_idx, (
        "backup deletion must come AFTER the sidecar cleanup — all post-replace "
        "verification needs to pass before we throw away the rollback target."
    )
    assert delete_idx < done_idx, (
        "backup deletion must come BEFORE the DONE state set. If it ran after, "
        "a crash between DONE and delete would leave a 'done' row pointing at a "
        "file whose .bak is still sitting alongside, wasting space."
    )


def test_backup_deletion_failure_is_non_fatal():
    """A failed os.remove on the bak must NOT raise out of finalize_upload.
    The new file is already in place; a stuck bak is just leftover for
    the next bulk purge. Raising here would mark the row ERROR for what
    is effectively a cleanup-step failure on an already-shipped file."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg.finalize_upload)
    # Locate the deletion block and confirm it's wrapped in a try/except.
    delete_idx = src.find("os.remove(backup_path)")
    assert delete_idx >= 0

    # Walk back 200 chars to find a try: and forward 300 chars to find
    # an except OSError handler — both must exist for the deletion to
    # be non-fatal.
    window = src[max(0, delete_idx - 200): delete_idx + 400]
    assert "try:" in window, "backup deletion must be wrapped in try/except"
    assert "except OSError" in window, (
        "must catch OSError specifically (PermissionError, ENOENT etc.) so "
        "a bak we can't delete doesn't fail the whole encode."
    )


def test_replace_log_no_longer_claims_backup_is_kept_long_term():
    """The replace log used to say 'backup kept at .original.bak' which
    misled subsequent maintainers into thinking the bak was a long-lived
    artefact. Now that the bak gets auto-deleted, the log message must
    not falsely imply persistence."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg.finalize_upload)
    assert "backup kept at .original.bak" not in src, (
        "stale log message — the bak is no longer kept long-term. Either "
        "drop the suffix or say 'backup kept for verification' so the "
        "transient nature is clear."
    )
