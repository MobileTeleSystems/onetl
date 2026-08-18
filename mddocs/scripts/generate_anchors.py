# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""
Generate anchors for all headings in docs/ following the pattern:
  {prefix}-{file_path_id}-{heading_id}

Where:
  prefix       = configurable via --prefix (default: DBR-onetl)
  file_path_id = relative path without .md extension, slashes replaced with dashes, lowercased
  heading_id   = heading text without existing { #anchor }, spaces replaced with dashes, lowercased

For duplicate heading texts within a file, all occurrences get a numeric suffix: -0, -1, -2, ...
For headings with an existing { #old } anchor, all [text][old] references in all files
are updated to [text][new].
"""

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import NamedTuple

HEADING_RE = re.compile(r"^(#{1,6}) (.+)$")
ANCHOR_IN_HEADING_RE = re.compile(r"\s*\{\s*#([^}]+?)\s*\}\s*$")
REF_LINK_RE = re.compile(r"\]\[([^\]]+)\]")
SNIPPET_RE = re.compile(r'--8<--\s*["\']([^"\']+)["\']')
EMOJI_RE = re.compile(
    r"[\U0001F600-\U0001F64F"  # emoticons
    r"\U0001F300-\U0001F5FF"  # misc symbols & pictographs
    r"\U0001F680-\U0001F6FF"  # transport & map
    r"\U0001F700-\U0001F9FF"  # alchemical, geometric, arrows, supplemental
    r"\U0001FA00-\U0001FAFF"  # chess, symbols & pictographs extended-a
    r"\U00002702-\U000027B0"  # dingbats
    r"\U000024C2-\U0001F251"  # enclosed characters
    r"]"
)
DATE_RE = re.compile(r"\s*\(\d{4}(?:-\d{2}(?:-\d{2})?)?\)")

Heading = tuple[int, str, str, str | None]  # (line_idx, level, display, existing_anchor)


class FileInfo(NamedTuple):
    lines: list[str]
    headings: list[Heading]
    anchors: dict[int, str]  # line_idx → anchor id (mutable dict inside immutable tuple)


# ---------------------------------------------------------------------------
# Anchor ID construction
# ---------------------------------------------------------------------------


def file_path_to_parts(fp: Path, docs_dir: Path) -> list[str]:
    """Return normalized, deduplicated path segments used to build the anchor."""
    parts = [p.replace("_", "-").replace(".", "-").lower() for p in fp.relative_to(docs_dir).with_suffix("").parts]
    if parts and parts[-1] == "index":
        parts = parts[:-1]
    # Skip a segment if its info is already carried by the remainder
    result = []
    for i, part in enumerate(parts):
        remaining = "-".join(parts[i + 1 :])
        if remaining == part or remaining.startswith(part + "-"):
            continue
        result.append(part)
    return result


def heading_text_to_id(display_text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", display_text)  # strip backtick spans
    text = EMOJI_RE.sub("", text)
    text = DATE_RE.sub("", text)
    text = text.strip().lower().replace(" ", "-").replace("_", "-").replace(".", "-")
    text = re.sub(r"[^\w\-]", "", text)
    return re.sub(r"-{2,}", "-", text).strip("-")


def make_anchor(prefix: str, file_parts: list[str], heading_base: str) -> str:
    """Build anchor, stripping any prefix of heading_base that repeats a tail of file_parts."""
    file_id = "-".join(file_parts)
    if not heading_base:
        return f"{prefix}-{file_id}"
    # Compare against proper segment boundaries, not arbitrary dash-splits of file_id
    for i in range(len(file_parts)):
        tail = "-".join(file_parts[i:])
        if heading_base == tail:
            return f"{prefix}-{file_id}"
        if heading_base.startswith(tail + "-"):
            return f"{prefix}-{file_id}-{heading_base[len(tail) + 1 :]}"
    return f"{prefix}-{file_id}-{heading_base}"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def split_heading(raw: str) -> tuple[str, str | None]:
    """Return (display_text, existing_anchor_or_None) from a raw heading text."""
    m = ANCHOR_IN_HEADING_RE.search(raw)
    return ANCHOR_IN_HEADING_RE.sub("", raw).strip(), (m.group(1).strip() if m else None)


def parse_file(content: str) -> tuple[list[Heading], list[str]]:
    """Parse content in a single pass outside code blocks.
    Returns (headings, snippet_paths).
    """
    headings: list[Heading] = []
    snippets: list[str] = []
    in_code_block = False
    for i, line in enumerate(content.split("\n")):
        if line.startswith(("```", "~~~")):
            in_code_block = not in_code_block
        if in_code_block:
            continue
        m = HEADING_RE.match(line)
        if m:
            display, existing = split_heading(m.group(2).strip())
            headings.append((i, m.group(1), display, existing))
        snippets.extend(SNIPPET_RE.findall(line))
    return headings, snippets


def generate_new_anchors(prefix: str, file_parts: list[str], headings: list[Heading]) -> dict[int, str]:
    """Returns {line_idx: new_anchor_id}."""
    bases = {line_idx: heading_text_to_id(display) for line_idx, _, display, _ in headings}
    base_counts = Counter(bases.values())
    seen: dict[str, int] = defaultdict(int)
    result: dict[int, str] = {}
    for line_idx, base in bases.items():
        anchor = make_anchor(prefix, file_parts, base)
        if base_counts[base] > 1:
            anchor = f"{anchor}-{seen[base]}"
            seen[base] += 1
        result[line_idx] = anchor
    return result


# ---------------------------------------------------------------------------
# Main pipeline steps
# ---------------------------------------------------------------------------


def parse_all_files(all_files: list[Path], docs_dir: Path, prefix: str) -> dict[Path, FileInfo]:
    file_data: dict[Path, FileInfo] = {}
    for fp in all_files:
        content = fp.read_text(encoding="utf-8")
        headings, snippets = parse_file(content)
        if snippets:
            print(
                f"WARNING: {fp.relative_to(docs_dir)} uses snippets {snippets} — included headings won't be processed",
                file=sys.stderr,
            )
        file_data[fp] = FileInfo(
            lines=content.split("\n"),
            headings=headings,
            anchors=generate_new_anchors(prefix, file_path_to_parts(fp, docs_dir), headings),
        )
    return file_data


def build_remappings(file_data: dict[Path, FileInfo]) -> tuple[dict[str, str], int]:
    """Build old-anchor → new-anchor mapping from explicit and auto-generated anchors.
    Returns (remappings, up_to_date_count) where up_to_date_count is the number of
    headings whose existing anchor already matches the generated one (idempotency check).
    """
    remappings: dict[str, str] = {}
    # auto_map: auto-generated anchor id → new anchor, or None if ambiguous across files
    auto_map: dict[str, str | None] = {}
    up_to_date = 0

    for info in file_data.values():
        for line_idx, _, display, existing in info.headings:
            new = info.anchors[line_idx]

            if existing:
                if existing == new:
                    up_to_date += 1  # anchor already correct — script is idempotent here
                elif existing in remappings and remappings[existing] != new:
                    print(f"WARNING: anchor {existing!r} conflict — skipping", file=sys.stderr)
                else:
                    remappings[existing] = new

            auto = heading_text_to_id(display)
            if auto != new and auto not in remappings:
                if auto in auto_map and auto_map[auto] != new:
                    auto_map[auto] = None  # same auto-id resolves to different anchors
                elif auto not in auto_map:
                    auto_map[auto] = new

    ambiguous = {k for k, v in auto_map.items() if v is None}
    if ambiguous:
        print(f"Ambiguous auto-anchors (skipped): {', '.join(sorted(ambiguous))}", file=sys.stderr)
    remappings.update({k: v for k, v in auto_map.items() if v is not None})
    return remappings, up_to_date


def resolve_duplicates(file_data: dict[Path, FileInfo], remappings: dict[str, str]) -> None:
    """Add numeric suffix to anchors that appear in more than one file."""
    all_entries = [
        (info.anchors[line_idx], fp, line_idx)
        for fp, info in sorted(file_data.items())
        for line_idx, *_ in info.headings
    ]
    duplicates = {a for a, c in Counter(a for a, *_ in all_entries).items() if c > 1}
    if not duplicates:
        return

    # Reverse index for O(1) remapping updates instead of O(n) scan per duplicate
    reverse: dict[str, list[str]] = defaultdict(list)
    for k, v in remappings.items():
        reverse[v].append(k)

    seen: dict[str, int] = defaultdict(int)
    for old_anchor, fp, line_idx in all_entries:
        if old_anchor in duplicates:
            new_anchor = f"{old_anchor}-{seen[old_anchor]}"
            seen[old_anchor] += 1
            file_data[fp].anchors[line_idx] = new_anchor
            for k in reverse.pop(old_anchor, []):
                remappings[k] = new_anchor
                reverse[new_anchor].append(k)
    print(f"Global duplicates resolved: {', '.join(sorted(duplicates))}", file=sys.stderr)


def write_files(
    file_data: dict[Path, FileInfo],
    remappings: dict[str, str],
    *,
    dry_run: bool,
) -> tuple[int, int]:
    """Write updated files. Returns (changed, unchanged) counts."""
    changed = unchanged = 0
    for fp, info in file_data.items():
        lines = list(info.lines)
        for line_idx, level, display, _ in info.headings:
            lines[line_idx] = f"{level} {display} {{ #{info.anchors[line_idx]} }}"
        new_content = REF_LINK_RE.sub(
            lambda m: f"][{remappings.get(m.group(1), m.group(1))}]",
            "\n".join(lines),
        )
        if new_content == "\n".join(info.lines):
            unchanged += 1
            continue
        changed += 1
        if not dry_run:
            fp.write_text(new_content, encoding="utf-8")
    return changed, unchanged


# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate structured anchors for all MkDocs headings.")
    parser.add_argument(
        "docs_dir",
        nargs="?",
        type=Path,
        default=Path("mddocs/docs"),
        help="Path to docs directory (default: mddocs/docs relative to repo root)",
    )
    parser.add_argument(
        "--prefix",
        default="DBR-onetl",
        help="Anchor prefix (default: DBR-onetl)",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Preview changes without writing files",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print all anchor remappings",
    )
    verbosity.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Print only the final summary line",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    docs_dir: Path = args.docs_dir
    prefix: str = args.prefix
    dry_run: bool = args.dry_run
    verbose: bool = args.verbose
    quiet: bool = args.quiet

    if not docs_dir.is_dir():
        print(f"Error: docs directory not found: {docs_dir}", file=sys.stderr)
        sys.exit(1)

    all_files = sorted(docs_dir.rglob("*.md"))
    if not all_files:
        print(f"No .md files found in {docs_dir}", file=sys.stderr)
        sys.exit(1)

    file_data = parse_all_files(all_files, docs_dir, prefix)
    remappings, up_to_date = build_remappings(file_data)
    resolve_duplicates(file_data, remappings)

    if not quiet:
        print(f"Files processed  : {len(file_data)}")
        print(f"Anchor remappings: {len(remappings)}")
        if up_to_date:
            print(f"Already up to date: {up_to_date} anchors (idempotent)")
    if verbose:
        for old, new in sorted(remappings.items()):
            print(f"  {old!r:50s} → {new!r}")

    changed, unchanged = write_files(file_data, remappings, dry_run=dry_run)
    action = "Would change" if dry_run else "Changed"
    print(f"{action}: {changed} files, {unchanged} unchanged.")


if __name__ == "__main__":
    main()
