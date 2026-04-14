from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".runtime_cache", "__pycache__", ".venv", "node_modules"}
BAD_SNIPPETS = ("\u00e2\u201a\u00ac", "\u00c3\u00a2", "\uFFFD")


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.is_dir():
            if path.name in SKIP_DIRS:
                # Skip entire subtree.
                files.extend([])
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".pdf"}:
            continue
        files.append(path)
    return files


def test_no_mojibake_chars() -> None:
    offenders: list[str] = []
    for path in _iter_files(REPO_ROOT):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if not any(snippet in text for snippet in BAD_SNIPPETS):
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            if any(snippet in line for snippet in BAD_SNIPPETS):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{idx}: {line.strip()}")
    assert not offenders, "Found mojibake characters:\n" + "\n".join(offenders)
