#!/usr/bin/env python3
"""Release helper for CHANGELOG.md (Keep a Changelog format).

    changelog.py release VERSION DATE [FILE]  move [Unreleased] to [VERSION] - DATE
    changelog.py notes VERSION [FILE]         print the section of VERSION
    changelog.py notes Unreleased [FILE]      print the [Unreleased] section

`release` fails when [Unreleased] is empty, so a release always says what
changed. It starts a new empty [Unreleased] section and updates the compare
links at the bottom.
"""
import re
import sys

HEADING = re.compile(r"^## \[([^\]]+)\]")
LINK = re.compile(r"^\[([^\]]+)\]: (\S+)$")


class ChangelogError(Exception):
    pass


def section(text, name):
    """Return the body of the `## [name]` section, stripped."""
    lines = text.split("\n")
    start = None
    for i, line in enumerate(lines):
        m = HEADING.match(line)
        if start is None:
            if m and m.group(1) == name:
                start = i + 1
        elif m or LINK.match(line):
            return "\n".join(lines[start:i]).strip()
    if start is None:
        raise ChangelogError(f"no ## [{name}] section")
    return "\n".join(lines[start:]).strip()


def release(text, version, date):
    if any(HEADING.match(l) and HEADING.match(l).group(1) == version for l in text.split("\n")):
        raise ChangelogError(f"## [{version}] already exists")
    if not section(text, "Unreleased"):
        raise ChangelogError("## [Unreleased] is empty: add the changes of this release first")
    text = text.replace("## [Unreleased]", f"## [Unreleased]\n\n## [{version}] - {date}", 1)

    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = LINK.match(line)
        if m and m.group(1) == "Unreleased":
            cm = re.match(r"^(.*)/compare/(\S+)\.\.\.HEAD$", m.group(2))
            if not cm:
                raise ChangelogError("the [Unreleased] link is not a .../compare/<tag>...HEAD URL")
            base, prev = cm.group(1), cm.group(2)
            lines[i] = f"[Unreleased]: {base}/compare/v{version}...HEAD"
            lines.insert(i + 1, f"[{version}]: {base}/compare/{prev}...v{version}")
            return "\n".join(lines)
    raise ChangelogError("no [Unreleased] link at the bottom")


def main(argv):
    if len(argv) >= 3 and argv[1] == "release":
        path = argv[4] if len(argv) > 4 else "CHANGELOG.md"
        with open(path, encoding="utf-8") as f:
            text = f.read()
        out = release(text, argv[2], argv[3])
        with open(path, "w", encoding="utf-8") as f:
            f.write(out)
        return 0
    if len(argv) >= 3 and argv[1] == "notes":
        path = argv[3] if len(argv) > 3 else "CHANGELOG.md"
        with open(path, encoding="utf-8") as f:
            print(section(f.read(), argv[2]))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except ChangelogError as e:
        print(f"CHANGELOG.md: {e}", file=sys.stderr)
        sys.exit(1)
