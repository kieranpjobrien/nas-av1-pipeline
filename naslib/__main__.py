"""Command-line entrypoint: ``python -m naslib {scan|plan|run|verify} [args]``.

The CLI is deliberately thin. Each subcommand maps to exactly one function
in the domain modules, with argparse flags for the knobs that actually
matter for operations. Anything that needs more than ~15 lines of
orchestration belongs in the domain module, not here.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from . import SCHEMA_VERSION
from .plan import build_plans, describe_pending, describe_plans
from .run import run_next
from .scan import DEFAULT_PROBE_WORKERS, scan_library
from .verify import verify_all, verify_file

# ---------------------------------------------------------------------------
# Sub-command implementations
# ---------------------------------------------------------------------------


def _cmd_scan(args: argparse.Namespace) -> int:
    """Run ``naslib scan`` with the requested flags."""
    stats = scan_library(
        incremental=bool(args.incremental),
        workers=int(args.workers),
        damage_check=bool(args.damage_check),
    )
    if args.damage_check:
        # damage-check prints its own summary inside scan_library.
        return 0 if stats.damaged == 0 else 2
    print(
        f"scan complete: scanned={stats.scanned} probed={stats.probed} "
        f"unchanged={stats.unchanged} failed={stats.probe_failed} "
        f"deleted={stats.deleted} damaged={stats.damaged}"
    )
    return 0 if stats.probe_failed == 0 else 1


def _cmd_plan(args: argparse.Namespace) -> int:
    """Run ``naslib plan`` with the requested flags."""
    stats, preview = build_plans(dry_run=bool(args.dry_run))
    tag = "would emit" if args.dry_run else "emitted"
    print(
        f"plan {tag}: {stats.emitted}  "
        f"(skipped existing={stats.skipped_existing} refused={stats.refused} "
        f"considered={stats.considered})"
    )
    if stats.by_action:
        print("by action:")
        print(describe_plans(preview))
    else:
        print("no pending work.")
    print(f"pending summary: {describe_pending()}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    """Run ``naslib run`` with the requested flags."""
    action_filter = args.action if args.action else None
    results = run_next(limit=int(args.limit), action_filter=action_filter)
    if not results:
        print("no pending plans matched the filter.")
        return 0
    any_failed = False
    for plan_id, result in results:
        print(f"plan {plan_id}: {result.status} :: {result.msg}")
        if result.status in ("failed", "refused"):
            any_failed = True
    return 1 if any_failed else 0


def _cmd_verify(args: argparse.Namespace) -> int:
    """Run ``naslib verify`` on one file or the whole AV1 library."""
    if args.all:
        ok, bad_count, bad = verify_all(av1_only=True)
        print(f"verify --all: ok={ok} bad={bad_count}")
        for report in bad:
            print(report.render())
        return 0 if bad_count == 0 else 1
    if not args.filepath:
        print("verify: provide a filepath or --all", file=sys.stderr)
        return 2
    report = verify_file(args.filepath)
    print(report.render())
    return 0 if report.ok else 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level argparse instance."""
    parser = argparse.ArgumentParser(
        prog="naslib",
        description=(f"Minimal, idempotent, single-writer NAS pipeline. Schema v{SCHEMA_VERSION}."),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # scan
    scan_p = sub.add_parser("scan", help="Walk the NAS and populate the inventory")
    scan_p.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files whose (size, mtime) already match the inventory.",
    )
    scan_p.add_argument(
        "--damage-check",
        action="store_true",
        help="Don't re-probe; only flag zero-audio or size-collapsed files.",
    )
    scan_p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_PROBE_WORKERS,
        help=f"Concurrent ffprobe workers (default {DEFAULT_PROBE_WORKERS}).",
    )
    scan_p.set_defaults(func=_cmd_scan)

    # plan
    plan_p = sub.add_parser("plan", help="Emit pending plans from the inventory")
    plan_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute plans without inserting them; prints a preview.",
    )
    plan_p.set_defaults(func=_cmd_plan)

    # run
    run_p = sub.add_parser("run", help="Execute pending plans")
    run_p.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum plans to execute this invocation (default 1).",
    )
    run_p.add_argument(
        "--action",
        choices=[
            "encode_av1",
            "transcode_audio",
            "mux_sub",
            "rename",
            "tag_tmdb",
            "delete_sidecar",
        ],
        default=None,
        help="Restrict the runner to plans of this action type.",
    )
    run_p.set_defaults(func=_cmd_run)

    # verify
    verify_p = sub.add_parser("verify", help="Probe a file (or the whole AV1 library) for compliance")
    verify_p.add_argument("filepath", nargs="?", help="Single filepath to verify.")
    verify_p.add_argument(
        "--all",
        action="store_true",
        help="Verify every row tagged as AV1 in the inventory.",
    )
    verify_p.set_defaults(func=_cmd_verify)

    return parser


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """Parse ``argv`` and dispatch to the selected subcommand.

    Returns the process exit code. ``0`` indicates success; non-zero
    indicates the subcommand reported a problem.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
