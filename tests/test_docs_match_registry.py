"""The generated tool table in docs/API.md must match the registry exactly
— the guard against the four-contradictory-tool-counts failure mode."""

import importlib.util
import subprocess
import sys
from pathlib import Path

V5_ROOT = Path(__file__).resolve().parents[1]


def test_tool_table_matches_registry():
    proc = subprocess.run(
        [sys.executable, str(V5_ROOT / "scripts" / "dump_tool_table.py"),
         "--check"],
        capture_output=True, text=True, cwd=str(V5_ROOT),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_version_is_51_line():
    spec = importlib.util.spec_from_file_location(
        "_v5_init", V5_ROOT / "ewsmcp" / "__init__.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.__version__.startswith("5.2."), mod.__version__
    pyproject = (V5_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'version = "{mod.__version__}"' in pyproject


def test_docs_describe_the_archive():
    design = (V5_ROOT / "DESIGN.md").read_text(encoding="utf-8")
    assert "## §Archive" in design
    assert "Phase 2 (not yet built)" not in design
    readme = (V5_ROOT / "README.md").read_text(encoding="utf-8")
    for key in ("ARCHIVE_DELETE_ENABLED", "ARCHIVE_AFTER_DAYS", "GEMINI_API_KEY",
                "ARCHIVE_GRACE_DAYS", "ARCHIVE_MAX_DELETE_PER_RUN",
                "ARCHIVE_MIN_FREE_GB", "ARCHIVE_CYCLE_SECONDS", "EMBED_DIMS"):
        assert key in readme, key
    changelog = (V5_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## [5.2.0a2]" in changelog


FORBIDDEN = ["SQLite", "FTS5", "700×", "700x", "norm_text",
             "sender_sigs", "EWS_CACHE_FOLDERS", "EWS_CACHE_WINDOW_DAYS",
             "Arabic", "bilingual"]


def test_docs_do_not_describe_removed_behaviour():
    offenders = []
    for name in ("DESIGN.md", "README.md", "docs/API.md"):
        text = (V5_ROOT / name).read_text(encoding="utf-8")
        for needle in FORBIDDEN:
            if needle in text:
                offenders.append(f"{name}: {needle}")
    assert not offenders, offenders


def test_semantic_mode_is_still_the_registry_truth():
    """The generated enum follows the registry; the prose must match what the
    registry actually has, in either direction."""
    text = (V5_ROOT / "docs" / "API.md").read_text(encoding="utf-8")
    assert "semantic" in text      # mode='semantic' is still a reserved enum value
    assert "find_similar" in text  # ...and the tool now exists (Gemini-backed)


def test_code_comments_do_not_mention_sqlite():
    proc = subprocess.run(
        ["grep", "-rn", "-i", "sqlite", "--include=*.py", "ewsmcp", "scripts"],
        capture_output=True, text=True, cwd=str(V5_ROOT))
    assert proc.stdout == "", proc.stdout
