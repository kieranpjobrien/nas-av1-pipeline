"""Pin the de-bloat backup auto-retire (2026-06-26).

With KEEP_BACKUPS off, the moved-aside original is deleted right after a verified swap
so reclaim backups never accumulate on the NAS. retire_backup does the delete (and
tidies the now-empty film folder); the main loop calls it only post-swap.
"""

from tools.reclaim_debloat import retire_backup


def test_retire_backup_removes_file_and_tidies_folder(tmp_path):
    film = tmp_path / "Film (2020)"
    film.mkdir()
    bak = film / "Film (2020).mkv"
    bak.write_bytes(b"x" * 16)
    assert retire_backup(str(bak)) is True
    assert not bak.exists()
    assert not film.exists()  # empty folder tidied


def test_retire_backup_none_returns_false():
    assert retire_backup(None) is False


def test_retire_backup_missing_returns_false(tmp_path):
    assert retire_backup(str(tmp_path / "nope.mkv")) is False
