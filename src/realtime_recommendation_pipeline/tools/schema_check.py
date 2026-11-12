"""Fail if an event schema change would break a released version (make schema-check).

The Schema Registry is the judge, so the check gives the answer the registry
would give a producer. In a subject of its own, set to the same rule as the
real one (FULL_TRANSITIVE), it registers in order every version released on
the base branch, then every version in the working tree. The registry refuses
any version that cannot read, or be read by, one before it, and the check
fails with the registry's reasons. Registering the released versions first
also catches a released file edited in place: the edited file is checked
against the version it changes. CI runs it on every pull request, against main.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from collections.abc import Sequence
from pathlib import PurePosixPath

from realtime_recommendation_pipeline.common.event_schemas import (
    VERSION_FILE,
    schema_text,
    versions,
)
from realtime_recommendation_pipeline.common.schema_registry import (
    IncompatibleSchema,
    RegistryError,
    RegistryUnavailable,
    SchemaRegistry,
)
from realtime_recommendation_pipeline.settings import SchemaRegistrySettings, load

SCHEMA_PATH = PurePosixPath(
    "src/realtime_recommendation_pipeline/schemas/viewing_event"
)

# (where it comes from, its text), in the order the registry should see them.
History = list[tuple[str, str]]


def released_history(ref: str) -> History:
    """The versions at a git ref, oldest first; none if the ref has none."""
    listed = subprocess.run(
        ["git", "ls-tree", "--name-only", f"{ref}:{SCHEMA_PATH}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if listed.returncode != 0:
        return []
    found = sorted(
        int(match.group(1))
        for name in listed.stdout.split()
        if (match := VERSION_FILE.fullmatch(name))
    )
    return [
        (
            f"v{version} on {ref}",
            subprocess.run(
                ["git", "show", f"{ref}:{SCHEMA_PATH / f'v{version}.avsc'}"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout,
        )
        for version in found
    ]


def working_history() -> History:
    return [(f"v{version}", schema_text(version)) for version in versions()]


def gaps(found: Sequence[int]) -> list[str]:
    """The version numbers missing from 1 to the newest."""
    missing = sorted(set(range(1, max(found, default=0) + 1)) - set(found))
    return [f"v{version}.avsc is missing" for version in missing]


def check(registry: SchemaRegistry, history: History) -> list[str]:
    """Register the history in a new subject; the reasons any version was refused."""
    subject = f"schema-check-{uuid.uuid4()}"
    problems = []
    try:
        registry.set_compatibility(subject)
        for label, text in history:
            try:
                registry.register(subject, text)
            except IncompatibleSchema:
                reasons = registry.problems(subject, text) or ["not compatible"]
                problems += [f"{label} breaks an earlier version: {r}" for r in reasons]
            except RegistryError as exc:
                problems.append(f"{label} was refused: {exc.message}")
    finally:
        registry.delete_subject(subject)
    return problems


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="the git ref whose schemas are released (default origin/main)",
    )
    args = parser.parse_args(argv)

    registry = SchemaRegistry(load(SchemaRegistrySettings).url)
    released = released_history(args.base_ref)
    history = released + working_history()
    try:
        problems = gaps(versions()) + check(registry, history)
    except RegistryUnavailable as exc:
        print(f"Schema check could not reach the registry: {exc}", file=sys.stderr)
        sys.exit(2)

    print(
        f"Checked {len(released)} released version(s) from {args.base_ref} and "
        f"{len(versions())} in the working tree, under FULL_TRANSITIVE."
    )
    for problem in problems:
        print(f"  {problem}")
    if problems:
        sys.exit(1)
    print("Every version can read, and be read by, every other.")


if __name__ == "__main__":
    main()
