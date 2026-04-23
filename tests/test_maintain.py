"""Tests for tools/maintain.py — the consolidated cleanup CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import maintain

# ---------------------------------------------------------------------------
# clean-names
# ---------------------------------------------------------------------------


def test_clean_names_dry_run_shows_plan_without_renaming(tmp_path, capsys):
    """clean-names dry-run must not rename files — only print the plan."""
    show_dir = tmp_path / "Breaking Bad" / "Season 1"
    show_dir.mkdir(parents=True)
    dirty = show_dir / "Breaking.Bad.S01E01.1080p.WEB-DL.x264-GROUP.mkv"
    dirty.write_text("x")

    args = maintain._build_parser().parse_args(
        ["clean-names", "--root", str(tmp_path)]
    )
    rc = args.func(args)
    assert rc == 0

    # File still exists with original name (no rename happened)
    assert dirty.exists()
    # Stdout should mention DRY RUN and the planned rename
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "Breaking.Bad.S01E01" in out


# ---------------------------------------------------------------------------
# dedupe
# ---------------------------------------------------------------------------


def test_dedupe_refuses_without_clean_sibling():
    """_classify_duplicates refuses to delete anything if all paths are dirty."""
    all_dirty = [
        Path(r"Show\Season 1\Show S01E01 ITA ENG.mkv"),
        Path(r"Show\Season 1\Show S01E01 MULTI.mkv"),
    ]
    result = maintain._classify_duplicates(all_dirty)
    assert result["delete"] == []
    assert result["keep"] is None
    assert result["review"] == all_dirty


def test_dedupe_accepts_single_clean_sibling():
    """If exactly one clean + one or more dirty, classifier flags dirty for delete."""
    clean = Path(r"Show\Season 1\Show S01E01.mkv")
    dirty = Path(r"Show\Season 1\Show S01E01 MULTI.mkv")
    result = maintain._classify_duplicates([clean, dirty])
    assert result["keep"] == clean
    assert result["delete"] == [dirty]
    assert result["review"] == []


# ---------------------------------------------------------------------------
# relocate
# ---------------------------------------------------------------------------


def test_relocate_refuses_on_collision(tmp_path, capsys):
    """Misfiled episode where Season N/<filename> already exists: no rename."""
    show_dir = tmp_path / "Chuck"
    show_dir.mkdir()
    season_dir = show_dir / "Season 2"
    season_dir.mkdir()

    # Original (misfiled in show root)
    misfiled = show_dir / "Chuck S02E12.mkv"
    misfiled.write_text("new")
    # Collision: same name already in Season 2
    existing = season_dir / "Chuck S02E12.mkv"
    existing.write_text("old")

    plan = maintain._find_misfiled_episodes(show_dir.parent)
    assert len(plan) == 1
    assert plan[0]["new_path"] == existing

    # Now actually run the execute path
    import argparse

    args = argparse.Namespace(execute=True)
    # Monkey-patch the NAS_SERIES root the cmd uses
    old_series = maintain.NAS_SERIES
    maintain.NAS_SERIES = tmp_path
    try:
        rc = maintain.cmd_relocate(args)
    finally:
        maintain.NAS_SERIES = old_series
    assert rc == 0

    # Collision means misfiled untouched
    assert misfiled.exists()
    assert existing.read_text() == "old"
    out = capsys.readouterr().out
    assert "collision" in out


# ---------------------------------------------------------------------------
# repair-sidecars
# ---------------------------------------------------------------------------


def test_repair_sidecars_matches_video_stem(tmp_path):
    """Orphan sidecar with only SxxExx stem gets paired to the matching video."""
    folder = tmp_path / "Show" / "Season 1"
    folder.mkdir(parents=True)
    video = folder / "Show S01E01 Pilot.mkv"
    video.write_text("v")
    # Orphan sidecar stem doesn't match video stem — just has SxxExx
    orphan = folder / "S01E01.en.srt"
    orphan.write_text("s")

    plans = maintain._pair_orphan_sidecars(folder)
    assert len(plans) == 1
    assert plans[0]["sidecar"] == orphan
    # Should rename to "Show S01E01 Pilot.en.srt"
    assert plans[0]["new_name"] == "Show S01E01 Pilot.en.srt"


def test_repair_sidecars_skips_already_paired(tmp_path):
    """Sidecar already sharing the video's stem is not planned for rename."""
    folder = tmp_path / "Show" / "Season 1"
    folder.mkdir(parents=True)
    video = folder / "Show S01E01.mkv"
    video.write_text("v")
    ok_sidecar = folder / "Show S01E01.en.srt"
    ok_sidecar.write_text("s")

    plans = maintain._pair_orphan_sidecars(folder)
    assert plans == []


# ---------------------------------------------------------------------------
# audit / queue-reencode
# ---------------------------------------------------------------------------


def test_audit_queues_reencode_for_non_compliant_file(tmp_path):
    """A file with a non-AV1 video codec lands in reencode.json after queue-reencode."""
    report = {
        "files": [
            {
                "filepath": r"\\KieranNAS\Media\Movies\Old Movie (1999)\Old Movie (1999).mkv",
                "filename": "Old Movie (1999).mkv",
                "library_type": "movie",
                "video": {"codec": "H.264", "codec_raw": "h264"},
                "audio_streams": [{"codec": "eac3", "codec_raw": "eac3", "language": "eng"}],
                "subtitle_streams": [{"language": "eng", "title": "", "codec": "subrip"}],
                "tmdb": {"tmdb_id": 42},
            },
        ]
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    reencode_path = tmp_path / "reencode.json"
    # Point maintain.CONTROL_DIR at tmp_path so the write lands inside the test
    old_ctrl = maintain.CONTROL_DIR
    maintain.CONTROL_DIR = tmp_path
    try:
        import argparse

        args = argparse.Namespace(report=str(report_path), limit=0, execute=True)
        rc = maintain.cmd_queue_reencode(args)
    finally:
        maintain.CONTROL_DIR = old_ctrl
    assert rc == 0

    data = json.loads(reencode_path.read_text(encoding="utf-8"))
    assert data["files"]
    assert r"\\KieranNAS\Media\Movies\Old Movie (1999)\Old Movie (1999).mkv" in data["files"]


def test_check_file_flags_scene_tags():
    """An otherwise-compliant entry with scene tags in filename is flagged."""
    entry = {
        "filepath": "x.mkv",
        "filename": "Movie.2020.1080p.BluRay.x264-GROUP.mkv",
        "library_type": "movie",
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
        "subtitle_streams": [{"language": "eng"}],
        "tmdb": {"tmdb_id": 1},
    }
    violations = maintain.check_file(entry, {"lossless_audio_codecs": []})
    assert any("scene tags" in v for v in violations)


# ---------------------------------------------------------------------------
# shim compat
# ---------------------------------------------------------------------------


SHIM_NAMES = [
    "tools.strip_tags",
    "tools.normalise_filenames",
    "tools.dedupe_episodes",
    "tools.relocate_misfiled_episodes",
    "tools.repair_sidecars",
    "tools.compliance",
]


@pytest.mark.parametrize("module", SHIM_NAMES)
def test_shim_scripts_still_work(module):
    """Each shim runs `python -m <module> --help` without ImportError."""
    project_root = Path(__file__).parent.parent
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        cwd=str(project_root),
        capture_output=True,
        text=True,
        timeout=30,
    )
    # argparse --help exits 0. We only care that import + argv translation
    # completed without crash, and that the text mentions something usage-like.
    assert result.returncode == 0, f"{module} failed: {result.stderr}"
    combined = (result.stdout + result.stderr).lower()
    assert "usage" in combined or "help" in combined
