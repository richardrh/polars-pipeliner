"""Command-line entry point for streaming pipeline run events."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .events import RunEvents, failure_fields
from .project import discover


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polars-pipeliner")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "validate"):
        subcommand = subcommands.add_parser(command)
        subcommand.add_argument(
            "project_root", nargs="?", default=".", metavar="PROJECT_ROOT"
        )
        subcommand.add_argument("--config", type=Path, metavar="PATH")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command not in {"run", "validate"}:
        return 2
    events = RunEvents(stream=sys.stdout)
    is_validation = args.command == "validate"
    started_event = "validation_started" if is_validation else "run_started"
    failure_event = "validation_failed" if is_validation else "run_failed"
    try:
        project = discover(
            args.project_root,
            config_path=args.config,
            _events=events,
            _started_event=started_event,
            _failure_event=failure_event,
            _validation_summary=is_validation,
        )
        if is_validation:
            project.validate()
        else:
            project.run()
    except Exception as error:
        if not events.emitted_failure:
            events.emit(failure_event, level="ERROR", **failure_fields(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
