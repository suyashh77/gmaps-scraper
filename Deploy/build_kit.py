"""
Assemble a volunteer collection kit from canonical sources.

    python Deploy/build_kit.py review --batches Data/batches/run1.db Data/batches/run2.db
    python Deploy/build_kit.py probe
    python Deploy/build_kit.py review --with-auth --force

Kits used to be hand-copied folders, and within four days the copy on the
volunteers' machines had drifted from the repository (patch_batch_path.py was
the symptom). A kit is now an output, never a source: fix the canonical code,
rebuild, ship. Every file in the kit is hashed into KIT_MANIFEST.json so a
returned folder can be checked against what was sent.

Secrets are copied only with --with-auth, and only into Deploy/build/, which
is gitignored.
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = Path(__file__).resolve().parent / "build"
MANIFEST_NAME = "KIT_MANIFEST.json"
AUTH_GLOB = "google_auth*.json"

# Never shipped, whatever folder they sit in.
EXCLUDE = (
    "__pycache__", "*.pyc", "*.log", "debug_*", "machine_id.txt", "pause.flag",
    "*.db", "*.db-wal", "*.db-shm", "google_auth*", ".venv", "tests",
)


@dataclass(frozen=True)
class CopyRule:
    """One source path (relative to the repo) copied to one path in the kit."""
    src: str
    dest: str


@dataclass(frozen=True)
class KitSpec:
    name: str
    code: tuple[CopyRule, ...]
    templates: str            # folder whose contents become the kit root
    auth_dest: str            # where --with-auth puts the session files
    queue_dest: str | None    # where the probe queue snapshot goes (probe kit only)


REVIEW_KIT = KitSpec(
    name="review_kit",
    code=(
        CopyRule("Scraper_v2/linux_scraper", "linux_scraper"),
        CopyRule("Scraper_v2/run_continuous.py", "run_continuous.py"),
    ),
    templates="Deploy/review_kit",
    auth_dest="auth",
    queue_dest=None,
)

# Layout mirrors the repo because Frame/probe.py resolves ../Scraper_v2 and
# ../Auth relative to itself.
PROBE_KIT = KitSpec(
    name="probe_kit",
    code=(
        CopyRule("Scraper_v2/linux_scraper", "Scraper_v2/linux_scraper"),
        CopyRule("Frame/probe.py", "Frame/probe.py"),
    ),
    templates="Deploy/probe_kit",
    auth_dest="Auth",
    queue_dest="Frame/build/probe.db",
)

KITS = {"review": REVIEW_KIT, "probe": PROBE_KIT}


def ro_uri(path: Path) -> str:
    """Read-only SQLite URI for a filesystem path.

    `f"file:{path}"` is not safe: `#`, `?` and `%` are legal in filenames but are
    reserved in a URI, and SQLite silently opened an empty database for a batch
    called `run#3.db`. as_uri() percent-encodes them.
    """
    return Path(path).resolve().as_uri() + "?mode=ro"


def _excluded(path: Path) -> bool:
    # Case-insensitive on every platform, so a kit built on Windows and one
    # built on Linux contain the same files.
    return any(fnmatch.fnmatchcase(part.lower(), pat.lower())
               for part in path.parts for pat in EXCLUDE)


def _copy_path(src: Path, dest: Path) -> None:
    """Copy a file or a tree, skipping anything on the exclude list."""
    if src.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return
    for item in src.rglob("*"):
        rel = item.relative_to(src)
        if item.is_file() and not _excluded(rel):
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)


def snapshot_db(src: Path, dest: Path, release_claims: bool = False) -> None:
    """Consistent, WAL-safe copy via VACUUM INTO; the source is never written."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    conn = sqlite3.connect(ro_uri(src), uri=True)
    try:
        conn.execute("VACUUM INTO ?", (str(dest),))
    finally:
        conn.close()
    if release_claims:
        # The kit starts on a fresh machine: whatever this machine had claimed
        # is not in flight there.
        snap = sqlite3.connect(dest)
        try:
            snap.execute("UPDATE probe SET status='pending' WHERE status='in_progress'")
            snap.commit()
        finally:
            snap.close()


def _copy_auth(root: Path, kit: Path, auth_dest: str) -> int:
    files = sorted((root / "Auth").glob(AUTH_GLOB))
    for f in files:
        _copy_path(f, kit / auth_dest / f.name)
    return len(files)


def _git_commit(root: Path) -> str:
    try:
        out = subprocess.run(["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def write_manifest(kit: Path, spec: KitSpec, root: Path) -> dict:
    files = []
    for p in sorted(kit.rglob("*")):
        if p.is_file() and p.name != MANIFEST_NAME:
            data = p.read_bytes()
            files.append({"path": p.relative_to(kit).as_posix(),
                          "sha256": hashlib.sha256(data).hexdigest(),
                          "bytes": len(data)})
    manifest = {
        "kit": spec.name,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit": _git_commit(root),
        "files": files,
    }
    (kit / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def _prepare_out(out: Path, force: bool) -> None:
    if out.exists() and any(out.iterdir()):
        if not force:
            raise FileExistsError(f"{out} is not empty; pass --force to rebuild it")
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)


def build(spec: KitSpec, root: Path, out: Path, *, batches: Iterable[Path] = (),
          with_auth: bool = False, force: bool = False) -> Path:
    """Assemble `spec` from `root` into `out` and return `out`."""
    _prepare_out(out, force)
    for rule in spec.code:
        _copy_path(root / rule.src, out / rule.dest)
    _copy_path(root / spec.templates, out)
    for batch in batches:
        snapshot_db(Path(batch), out / "batches" / Path(batch).name)
    if spec.queue_dest:
        snapshot_db(root / spec.queue_dest, out / spec.queue_dest, release_claims=True)
    if with_auth:
        n = _copy_auth(root, out, spec.auth_dest)
        print(f"  !! {n} live Google session file(s) copied into {out / spec.auth_dest}."
              " Do not share this folder.")
    write_manifest(out, spec, root)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Assemble a volunteer collection kit.",
                                 epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kit", choices=sorted(KITS))
    ap.add_argument("--out", type=Path, help=f"output folder (default {BUILD_DIR}/<kit>)")
    ap.add_argument("--root", type=Path, default=REPO_ROOT, help=argparse.SUPPRESS)
    ap.add_argument("--batches", nargs="*", type=Path, default=(),
                    help="batch DBs to snapshot into batches/ (review kit)")
    ap.add_argument("--with-auth", action="store_true",
                    help="copy Auth/google_auth_*.json into the kit")
    ap.add_argument("--force", action="store_true", help="rebuild over a non-empty --out")
    args = ap.parse_args(argv)

    spec = KITS[args.kit]
    out = args.out or (BUILD_DIR / spec.name)
    try:
        kit = build(spec, args.root, out, batches=args.batches,
                    with_auth=args.with_auth, force=args.force)
    except FileExistsError as exc:
        print(f"ERROR: {exc}")
        return 1
    n_files = sum(1 for p in kit.rglob("*") if p.is_file())
    print(f"built {spec.name}: {n_files} files in {kit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
