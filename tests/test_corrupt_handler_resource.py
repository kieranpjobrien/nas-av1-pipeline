"""Pin the 2026-07-14 corrupt_handler.resource() double-delete fix.

Radarr's ``DELETE /api/v3/moviefile/{id}`` (and Sonarr's episodefile
equivalent) removes the physical file from disk. The old resource() then
called ``os.remove(fp)`` unconditionally, which raised ``FileNotFoundError``
(WinError 2) because the file was already gone. That exception aborted
resource() BEFORE it triggered the re-grab search and cleared the state
row — so files ended up "deleted but never re-sourced", stranded in
flagged_corrupt limbo. This was the real cause of the WinError-2 storm in
corrupt_handler.log (Back to the Future, Bao, Major League, ...).

Fix: ``_remove_if_present`` swallows FileNotFoundError so resource() runs
to completion.
"""

from __future__ import annotations

import sqlite3

from tools import corrupt_handler, radarr


def test_remove_if_present_tolerates_missing(tmp_path):
    """The core fix: deleting an already-gone file must not raise."""
    corrupt_handler._remove_if_present(str(tmp_path / "does-not-exist.mkv"))


def test_remove_if_present_deletes_existing(tmp_path):
    f = tmp_path / "there.mkv"
    f.write_bytes(b"x")
    corrupt_handler._remove_if_present(str(f))
    assert not f.exists()


def test_resource_completes_after_arr_already_deleted_file(tmp_path, monkeypatch):
    """End-to-end: even when the file is already gone (arr deleted it), the
    re-grab search MUST still fire and the stale row MUST be cleared."""
    fp = str(tmp_path / "Fake Movie (2020).mkv")  # deliberately NOT created
    calls: dict[str, list] = {"delete": [], "search": []}
    monkeypatch.setattr(radarr, "find_movie_by_path",
                        lambda p: {"id": 42, "movieFile": {"id": 99}})
    monkeypatch.setattr(radarr, "_request",
                        lambda method, path, **kw: calls["delete"].append((method, path)))
    monkeypatch.setattr(radarr, "trigger_search",
                        lambda mid: (calls["search"].append(mid), {"id": 1})[1])

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE pipeline_files (filepath TEXT PRIMARY KEY, status TEXT)")
    con.execute("INSERT INTO pipeline_files VALUES (?, 'flagged_corrupt')", (fp,))
    con.commit()

    res = corrupt_handler.resource(fp, None, con)

    assert res == "RE-SOURCED (Radarr movie)"
    assert calls["search"] == [42], "re-grab search must fire even though the file was already gone"
    remaining = con.execute(
        "SELECT COUNT(*) FROM pipeline_files WHERE filepath=?", (fp,)).fetchone()[0]
    assert remaining == 0, "stale flagged_corrupt row must be cleared after resource()"
