"""Qualification-only operator recovery; not an installer operation or automatic downgrade.

The caller must first prove its owned host stopped gracefully. Keep the changed
full tree and original vault-lock inode while restoring a verified full backup.
"""

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

spec = importlib.util.spec_from_file_location(
    "maintenance", Path(__file__).with_name("browser-maintenance.py")
)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def copy_entries(source, destination, entries, preserve_lease=False):
    for entry in entries:
        relative = entry["path"]
        copied = destination / relative
        if preserve_lease and relative in ("", "vault.lock"):
            continue
        if entry["type"] == "directory":
            os.mkdir(copied, entry["mode"])
            os.chown(copied, entry["uid"], entry["gid"])
        else:
            source_fd = os.open(source / relative, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            target_fd = None
            try:
                target_fd = os.open(
                    copied, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, entry["mode"]
                )
                os.fchown(target_fd, entry["uid"], entry["gid"])
                with os.fdopen(source_fd, "rb", closefd=False) as reader, os.fdopen(
                    target_fd, "wb", closefd=False
                ) as writer:
                    shutil.copyfileobj(reader, writer, 1024 * 1024)
                    writer.flush()
                    os.fsync(target_fd)
            finally:
                os.close(source_fd)
                if target_fd is not None:
                    os.close(target_fd)
    for entry in reversed(entries):
        if entry["type"] == "directory":
            m.fsync_directory(destination / entry["path"])


def restore_locked(data, backup, preserved, uid, gid, expected_manifest):
    data, backup, preserved = Path(data), Path(backup), Path(preserved)
    m.require(not os.path.lexists(preserved), "backup-unavailable")
    m.require(
        data.parent == preserved.parent and backup / "data" != data,
        "ownership-unconfirmed",
    )
    content = m.private_read(backup / "inventory.json", os.getuid(), os.getgid(), 2 * 1024 * 1024)
    m.require(m.digest(content) == expected_manifest, "backup-unavailable")
    manifest = m.decode(content)
    before = m.inventory(data, uid, gid)
    expected = m.inventory(backup / "data", uid, gid)
    m.require(
        manifest["entries"] == expected
        and manifest["dataSnapshotSha256"] == m.digest(m.canonical(expected)),
        "backup-unavailable",
    )
    lock = data / "vault.lock"
    lease = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        original = os.fstat(lease)
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = lock.stat()
        m.require(
            (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino),
            "ownership-unconfirmed",
        )
        m.require(
            next(entry for entry in expected if entry["path"] == "vault.lock")
            == next(entry for entry in before if entry["path"] == "vault.lock"),
            "ownership-unconfirmed",
        )
        copy_entries(data, preserved, before)
        m.fsync_directory(data.parent)
        m.require(m.inventory(preserved, uid, gid) == before, "backup-unavailable")
        m.require(m.inventory(data, uid, gid) == before, "ownership-unconfirmed")
        for child in data.iterdir():
            if child.name == "vault.lock":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        copy_entries(backup / "data", data, expected, preserve_lease=True)
        root = next(entry for entry in expected if entry["path"] == "")
        os.chmod(data, root["mode"])
        os.chown(data, root["uid"], root["gid"])
        m.fsync_directory(data)
        m.require(m.inventory(data, uid, gid) == expected, "backup-unavailable")
        m.require(m.inventory(preserved, uid, gid) == before, "backup-unavailable")
        current = lock.stat()
        m.require(
            (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino),
            "ownership-unconfirmed",
        )
        return {
            "scope": "explicit operator full-backup recovery rehearsal",
            "originalLeaseInodePreserved": True,
            "restoredDataSnapshotSha256": m.digest(m.canonical(expected)),
            "preservedChangedDataSnapshotSha256": m.digest(m.canonical(before)),
            "backupInventorySha256": expected_manifest,
        }
    finally:
        os.close(lease)


def restore(data, backup, preserved, uid, gid, expected_manifest):
    lock = Path(data).parent / "installer.lock"
    m.private_read(lock, os.getuid(), os.getgid(), 1024)
    owner = os.open(lock, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        original = os.fstat(owner)
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = lock.stat()
        m.require(
            (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino),
            "ownership-unconfirmed",
        )
        result = restore_locked(data, backup, preserved, uid, gid, expected_manifest)
        current = lock.stat()
        m.require(
            (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino),
            "ownership-unconfirmed",
        )
        return {**result, "originalInstallerLockInodePreserved": True}
    finally:
        os.close(owner)


if __name__ == "__main__":
    try:
        data, backup, preserved, uid, gid, expected_manifest = sys.argv[1:]
        value = restore(data, backup, preserved, int(uid), int(gid), expected_manifest)
        print(json.dumps(value, sort_keys=True))
    except Exception:
        # Retain both predecessor and changed tree; never print paths or private file content.
        print("Operator recovery rehearsal failed; owned recovery evidence was retained.", file=sys.stderr)
        sys.exit(1)
