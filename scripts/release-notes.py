#!/usr/bin/env python3
"""Prepend aggregate release notes for Scry's virtual Cargo workspace."""

from __future__ import annotations

import re
import subprocess
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

HEADINGS = {
    "feat": "Features",
    "fix": "Bug Fixes",
    "perf": "Performance Improvements",
    "refactor": "Refactoring",
    "docs": "Documentation",
    "test": "Tests",
    "build": "Build",
    "ci": "CI",
    "chore": "Chores",
}
ORDER = list(HEADINGS) + ["other"]


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def latest_tag() -> str | None:
    tags = git("tag", "--list", "v[0-9]*", "--sort=-v:refname").splitlines()
    return tags[0] if tags else None


def subjects_since_release() -> list[str]:
    tag = latest_tag()
    revision = f"{tag}..HEAD^" if tag else "HEAD^"
    output = git("log", "--reverse", "--format=%s", revision)
    return output.splitlines() if output else []


def render(version: str, subjects: list[str]) -> str:
    groups: dict[str, list[str]] = defaultdict(list)
    conventional = re.compile(r"^([a-zA-Z]+)(?:\([^)]*\))?(!)?:\s+(.+)$")
    for subject in subjects:
        match = conventional.match(subject)
        if match:
            kind, breaking, text = match.groups()
            key = kind.lower() if kind.lower() in HEADINGS else "other"
            if breaking:
                text = f"**BREAKING:** {text}"
        else:
            key, text = "other", subject
        groups[key].append(text)

    lines = [f"## {version} ({date.today().isoformat()})", ""]
    for key in ORDER:
        entries = groups.get(key)
        if not entries:
            continue
        lines.extend([f"### {HEADINGS.get(key, 'Other')}", ""])
        lines.extend(f"- {entry}" for entry in entries)
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) != 2 or not re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", sys.argv[1]):
        raise SystemExit("usage: scripts/release-notes.py X.Y.Z")
    version = sys.argv[1]
    path = Path("CHANGELOG.md")
    existing = path.read_text() if path.exists() else ""
    header = "# Changelog\n\n"
    if existing.startswith(header):
        existing = existing[len(header) :]
    section = render(version, subjects_since_release())
    path.write_text(header + section + existing)


if __name__ == "__main__":
    main()
