"""Tests for Deploy/build_kit.py.

A kit is assembled from canonical sources into an empty output folder. It must
carry the code and templates, never carry runtime junk, never carry secrets
unless asked, snapshot databases WAL-safely, and describe itself in a manifest.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import build_kit  # noqa: E402


def _write(path: Path, text: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _make_db(path: Path, table: str, rows: list[tuple]) -> None:
    c = sqlite3.connect(path)
    c.execute("PRAGMA journal_mode=WAL")
    if table == "stores":
        c.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, status TEXT)")
        c.execute("CREATE TABLE reviews (id INTEGER PRIMARY KEY, store_id INTEGER)")
    else:
        c.execute("CREATE TABLE probe (store_id INTEGER PRIMARY KEY, status TEXT, machine_id TEXT)")
    placeholders = ",".join("?" * len(rows[0]))
    c.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    c.commit()          # leave pages in the -wal on purpose
    c.close()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    _write(root / "Scraper_v2" / "linux_scraper" / "__init__.py", "VERSION = '9'\n")
    _write(root / "Scraper_v2" / "linux_scraper" / "scraper.py", "x = 1\n")
    _write(root / "Scraper_v2" / "linux_scraper" / "__pycache__" / "scraper.cpython-312.pyc", "junk")
    _write(root / "Scraper_v2" / "linux_scraper" / "scraper.log", "junk")
    _write(root / "Scraper_v2" / "run_continuous.py", "y = 2\n")
    _write(root / "Scraper_v2" / "machine_id.txt", "abc123")
    _write(root / "Scraper_v2" / "google_auth_1.json", "{}")
    _write(root / "Frame" / "probe.py", "z = 3\n")
    _write(root / "Auth" / "google_auth_1.json", "{}")
    _write(root / "Auth" / "google_auth_2.json.banned", "{}")
    for name in ("setup.sh", "_run.sh", "run_machine1.sh", "collect_results.sh",
                 "README.md", "requirements.txt", ".gitignore"):
        _write(root / "Deploy" / "review_kit" / name, "#!/usr/bin/env bash\ntrue\n")
    for name in ("START.bat", "setup.bat", "run_probe.py", "README.md", "requirements.txt"):
        _write(root / "Deploy" / "probe_kit" / name, "rem\n")
    (root / "Data" / "batches").mkdir(parents=True)
    _make_db(root / "Data" / "batches" / "run1.db", "stores",
             [(1, "pending"), (2, "pending"), (3, "completed")])
    (root / "Frame" / "build").mkdir(parents=True)
    _make_db(root / "Frame" / "build" / "probe.db", "probe",
             [(1, "in_progress", "local"), (2, "pending", None), (3, "done", "local")])
    return root


def _names(kit: Path) -> set[str]:
    return {p.relative_to(kit).as_posix() for p in kit.rglob("*") if p.is_file()}


def _count(db: Path, sql: str) -> int:
    c = sqlite3.connect(build_kit.ro_uri(db), uri=True)
    try:
        return c.execute(sql).fetchone()[0]
    finally:
        c.close()


# -- review kit -------------------------------------------------------------

def test_review_kit_has_code_templates_and_manifest(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk")
    names = _names(kit)
    for expected in ("linux_scraper/__init__.py", "linux_scraper/scraper.py", "run_continuous.py",
                     "setup.sh", "_run.sh", "run_machine1.sh", "collect_results.sh",
                     "README.md", "requirements.txt", ".gitignore", "KIT_MANIFEST.json"):
        assert expected in names


def test_review_kit_carries_no_runtime_junk(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk")
    names = _names(kit)
    assert not any("__pycache__" in n or n.endswith((".pyc", ".log")) for n in names)
    assert "machine_id.txt" not in names


def test_review_kit_snapshots_batches_without_wal_sidecar(repo: Path, tmp_path: Path) -> None:
    src = repo / "Data" / "batches" / "run1.db"
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk", batches=(src,))
    out = kit / "batches" / "run1.db"
    assert out.exists()
    assert not (kit / "batches" / "run1.db-wal").exists()
    assert _count(out, "SELECT COUNT(*) FROM stores") == 3


def test_secrets_only_with_flag(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk")
    assert not any("google_auth" in n for n in _names(kit))
    kit2 = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk2", with_auth=True)
    names = _names(kit2)
    assert "auth/google_auth_1.json" in names
    assert not any(n.endswith(".banned") for n in names)


def test_manifest_hashes_match_files(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk")
    manifest = json.loads((kit / "KIT_MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["kit"] == "review_kit"
    assert "git_commit" in manifest and "built_at" in manifest
    assert manifest["files"], "manifest lists no files"
    for entry in manifest["files"]:
        data = (kit / entry["path"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == entry["sha256"]
        assert len(data) == entry["bytes"]
    assert not any(e["path"] == "KIT_MANIFEST.json" for e in manifest["files"])


def test_refuses_nonempty_output_without_force(repo: Path, tmp_path: Path) -> None:
    out = tmp_path / "rk"
    build_kit.build(build_kit.REVIEW_KIT, repo, out)
    with pytest.raises(FileExistsError):
        build_kit.build(build_kit.REVIEW_KIT, repo, out)
    build_kit.build(build_kit.REVIEW_KIT, repo, out, force=True)
    assert (out / "KIT_MANIFEST.json").exists()


def test_review_kit_package_imports(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.REVIEW_KIT, repo, tmp_path / "rk")
    code = f"import sys; sys.path.insert(0, {str(kit)!r}); import linux_scraper"
    assert subprocess.call([sys.executable, "-c", code]) == 0


# -- probe kit --------------------------------------------------------------

def test_probe_kit_preserves_layout_probe_needs(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.PROBE_KIT, repo, tmp_path / "pk")
    names = _names(kit)
    for expected in ("Frame/probe.py", "Scraper_v2/linux_scraper/__init__.py",
                     "Frame/build/probe.db", "START.bat", "run_probe.py", "KIT_MANIFEST.json"):
        assert expected in names
    assert not any("google_auth" in n for n in names)


def test_probe_kit_snapshot_releases_claims_and_leaves_source_alone(
        repo: Path, tmp_path: Path) -> None:
    src = repo / "Frame" / "build" / "probe.db"
    kit = build_kit.build(build_kit.PROBE_KIT, repo, tmp_path / "pk")
    snap = kit / "Frame" / "build" / "probe.db"
    assert _count(snap, "SELECT COUNT(*) FROM probe WHERE status='in_progress'") == 0
    assert _count(snap, "SELECT COUNT(*) FROM probe") == 3
    assert _count(src, "SELECT COUNT(*) FROM probe WHERE status='in_progress'") == 1


def test_probe_kit_auth_lands_in_capital_auth(repo: Path, tmp_path: Path) -> None:
    kit = build_kit.build(build_kit.PROBE_KIT, repo, tmp_path / "pk", with_auth=True)
    assert "Auth/google_auth_1.json" in _names(kit)


# -- CLI --------------------------------------------------------------------

def test_cli_builds_review_kit(repo: Path, tmp_path: Path) -> None:
    out = tmp_path / "cli_rk"
    rc = build_kit.main(["review", "--root", str(repo), "--out", str(out)])
    assert rc == 0
    assert (out / "run_continuous.py").exists()


# -- regressions from review ------------------------------------------------

def test_snapshot_handles_uri_reserved_characters_in_path(tmp_path: Path) -> None:
    # '#' is legal in an NTFS/ext4 filename but starts a fragment in a file: URI;
    # a naive f"file:{path}" silently opened an empty database.
    src = tmp_path / "run#3 (copy) 100%.db"
    _make_db(src, "stores", [(1, "pending")])
    dest = tmp_path / "out" / "snap.db"
    build_kit.snapshot_db(src, dest)
    assert _count(dest, "SELECT COUNT(*) FROM stores") == 1


def test_snapshot_includes_frames_still_in_the_wal(tmp_path: Path) -> None:
    src = tmp_path / "live.db"
    _make_db(src, "stores", [(1, "pending")])
    writer = sqlite3.connect(src)
    try:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO stores VALUES (2, 'pending')")
        writer.commit()                         # committed, but only in the -wal
        assert (tmp_path / "live.db-wal").exists()
        dest = tmp_path / "out" / "snap.db"
        build_kit.snapshot_db(src, dest)
        assert _count(dest, "SELECT COUNT(*) FROM stores") == 2
        assert not (tmp_path / "out" / "snap.db-wal").exists()
    finally:
        writer.close()
