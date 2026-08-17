"""Enrichment must not clobber the report the pipeline is also writing.

2026-08-17: tools/one_off_enrich_and_fill ran twice against a 1,568-entry TMDb
gap and closed NONE of it, reporting success both times.

It read the whole report, held it for the ~40 minutes of TMDb lookups, then
wrote it back wholesale with os.replace(). Every other writer goes through
report_lock.patch_report. So the pipeline's writes during that window were
clobbered, and the pipeline's own copy clobbered the enrichment right back -
rule 13, and the shape of the 2026-04-29 report wipe.

A second defect hid the first: ``except Exception: pass`` around every lookup,
so an API failure was indistinguishable from "nothing to do".
"""

import ast
import inspect
from pathlib import Path

import tools.one_off_enrich_and_fill as enrich

SRC = Path(inspect.getfile(enrich)).read_text(encoding="utf-8")


class TestWritesGoThroughTheLock:
    def test_no_bare_os_replace_on_the_report(self):
        """The whole-file overwrite is what lost the writes."""
        tree = ast.parse(SRC)
        calls = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "replace"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "os"
        ]
        assert not calls, "media_report.json must only be written via report_lock"

    def test_uses_patch_report(self):
        assert "patch_report" in SRC
        assert "from tools.report_lock import" in SRC

    def test_merge_is_keyed_by_filepath(self):
        """Merging by index would corrupt the report if the pipeline added or
        removed entries while the lookups were running."""
        src = inspect.getsource(enrich._merge)
        assert "filepath" in src

    def test_merge_does_not_overwrite_existing_tmdb(self):
        """The pipeline may have enriched the same entry meanwhile - respect it."""
        src = inspect.getsource(enrich._merge)
        assert "tmdb_id" in src and "not" in src


class TestFailuresAreVisible:
    def test_lookup_failures_are_counted_not_swallowed(self):
        src = inspect.getsource(enrich.main)
        assert "failures" in src, "lookup errors must be counted"
        assert "error breakdown" in src or "errors:" in src, "and reported"

    def test_no_silent_pass_in_the_lookup_loop(self):
        src = inspect.getsource(enrich.main)
        # a bare `except Exception: pass` is what made a broken API key look
        # identical to a fully-enriched library
        assert "pass" not in [ln.strip() for ln in src.splitlines()]

    def test_lookup_raises_rather_than_returning_none_on_error(self):
        """None must mean 'no TMDb match', never 'the call blew up'."""
        doc = (enrich._lookup.__doc__ or "").lower()
        assert "raises" in doc
