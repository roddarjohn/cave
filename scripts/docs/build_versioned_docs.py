"""Build versioned Sphinx docs for all tags and main.

Each ``pgcraft-v*`` git tag and the ``main`` branch get their own
subdirectory under ``docs/_build/html/``.  A ``versions.json`` file
is written at the root so the sidebar version selector can discover
them.

Tag builds run in parallel and are skipped when the output directory
already exists (cache hit).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT = ROOT / "docs" / "_build" / "html"
SPHINX_BUILD = [
    "uv",
    "run",
    "--group",
    "docs",
    "sphinx-build",
    "-b",
    "html",
]
TAG_PREFIX = "pgcraft-"
MAX_WORKERS = min(4, (os.cpu_count() or 1))


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _get_tags() -> list[str]:
    """Return version tags sorted newest-first."""
    raw = _git("tag", "-l", "pgcraft-v*", "--sort=-version:refname")
    return [t for t in raw.splitlines() if t.strip()]


def _build(source_docs: Path, dest: Path) -> bool:
    """Run sphinx-build.  Returns True on success."""
    dest.mkdir(parents=True, exist_ok=True)
    # Ensure _generated dir exists so includes don't fail
    (source_docs / "_generated").mkdir(exist_ok=True)
    result = subprocess.run(
        [*SPHINX_BUILD, str(source_docs), str(dest)],
        cwd=source_docs.parent,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _build_main() -> None:
    """Build docs from the current working tree as 'main'."""
    print("==> Building main")
    dest = OUTPUT / "main"
    # Always rebuild main from the working tree
    if dest.exists():
        shutil.rmtree(dest)
    if not _build(ROOT / "docs", dest):
        print("ERROR: main build failed", file=sys.stderr)
        sys.exit(1)


def _build_tag(tag: str) -> str | None:
    """Build docs for a single git tag.  Returns label on success."""
    label = tag.removeprefix(TAG_PREFIX)
    dest = OUTPUT / label

    # Cache: skip if already built
    if dest.exists() and (dest / "index.html").exists():
        print(f"==> {label} (cached)")
        return label

    print(f"==> Building {label}")
    tmpdir = tempfile.mkdtemp(prefix=f"pgcraft-docs-{label}-")
    try:
        subprocess.run(
            ["git", "worktree", "add", "--detach", tmpdir, tag],
            cwd=ROOT,
            capture_output=True,
            check=True,
        )
        docs_dir = Path(tmpdir) / "docs"
        if not docs_dir.exists():
            print(f"    Skipping {label}: no docs/ directory")
            return None
        if not _build(docs_dir, dest):
            print(f"    Warning: build failed for {label}, skipping")
            return None
    except subprocess.CalledProcessError as exc:
        print(
            f"    Warning: could not checkout {label}: {exc}",
            file=sys.stderr,
        )
        return None
    else:
        return label
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", tmpdir],
            cwd=ROOT,
            capture_output=True,
            check=False,
        )
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> None:
    """Entry point."""
    OUTPUT.mkdir(parents=True, exist_ok=True)

    _build_main()
    versions = ["main"]

    tags = _get_tags()
    if tags:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_build_tag, tag): tag for tag in tags}
            for future in as_completed(futures):
                label = future.result()
                if label:
                    versions.append(label)

    # Maintain newest-first order for the version selector
    tag_order = [t.removeprefix(TAG_PREFIX) for t in tags]
    versions.sort(key=lambda v: tag_order.index(v) if v in tag_order else -1)

    # Write versions.json for the JS version selector
    (OUTPUT / "versions.json").write_text(json.dumps(versions, indent=2) + "\n")

    # Root redirect to main
    redirect = ROOT / "docs" / "_templates" / "redirect.html"
    shutil.copy(redirect, OUTPUT / "index.html")

    # Disable Jekyll processing on GitHub Pages
    (OUTPUT / ".nojekyll").touch()

    print(f"==> Done. Built versions: {', '.join(versions)}")


if __name__ == "__main__":
    main()
