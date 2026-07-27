"""Log messages must never be able to kill a worker thread.

2026-07-28: a log line containing a non-ASCII arrow raised UnicodeEncodeError
from the Windows console's cp1252 codec. logging lets that propagate out of
emit(), so it killed the GPU worker thread. The other threads carried on
logging happily, so the process looked alive while no encode had run for 1.5
hours - and because the FILE handler is UTF-8, the offending message appeared
in pipeline.log with no sign of the crash.

34 log calls across pipeline/ carry non-ASCII characters. Policing them one
by one is whack-a-mole; the stream itself must be unable to raise.
"""

import io
import logging

import pytest

from pipeline.__main__ import setup_logging


@pytest.fixture(autouse=True)
def _clean_root_logger():
    """logging.basicConfig is a no-op when the root logger already has
    handlers, so each test needs a clean slate."""
    root = logging.getLogger()
    saved = root.handlers[:]
    root.handlers = []
    yield
    for h in root.handlers:
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass
    root.handlers = saved


def test_non_ascii_log_message_does_not_raise(tmp_path, capsys):
    """The exact 2026-07-28 signature: an arrow in a log message."""
    setup_logging(str(tmp_path))
    logging.info("  Under quality floor → parked: Some Show S01E01.mkv")
    logging.info("  Auto-reset flagged_corrupt → pending")
    logging.info("café — em-dash and accents éèü")
    # Reaching here at all is the assertion: no UnicodeEncodeError escaped.
    assert (tmp_path / "pipeline.log").exists()


def test_message_survives_into_the_log_file(tmp_path):
    """The non-ASCII message must still reach the log file intact.

    Emits through the configured FileHandler directly rather than the root
    logger: pytest's logging plugin swaps root handlers around, which would
    make this a test of pytest rather than of setup_logging.
    """
    setup_logging(str(tmp_path))
    file_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)]
    assert file_handlers, "setup_logging must install a FileHandler"
    rec = logging.LogRecord("t", logging.INFO, __file__, 1, "quality floor → parked: Marker123", None, None)
    for h in file_handlers:
        h.emit(rec)
        h.flush()
    text = (tmp_path / "pipeline.log").read_text(encoding="utf-8")
    assert "Marker123" in text
    assert "→" in text, "the file handler is UTF-8 and must keep the character"


def test_cp1252_stream_cannot_kill_the_emitting_thread():
    """A handler bound to a cp1252 stream must degrade, not explode.

    Simulates the Windows console directly rather than relying on the host
    platform's default encoding.
    """
    buf = io.BytesIO()
    stream = io.TextIOWrapper(buf, encoding="cp1252", errors="replace")
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("encoding_probe")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    logger.info("floor → parked")  # would raise with errors='strict'
    handler.flush()
    assert b"floor" in buf.getvalue()


def test_strict_cp1252_would_have_raised():
    """Pins WHY the fix is needed: with errors='strict' this really does blow
    up, so the guard above is load-bearing rather than decorative."""
    buf = io.BytesIO()
    stream = io.TextIOWrapper(buf, encoding="cp1252", errors="strict")
    with pytest.raises(UnicodeEncodeError):
        stream.write("floor → parked")
        stream.flush()
