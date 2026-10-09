"""Focused transaction tests: real journal, copy, fsync, lock and retained data.
External Docker/HTTP effects are controlled; whole shell and native tests own delivery.
"""

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "maintenance", Path(__file__).resolve().parents[2] / "tools/package/browser-maintenance.py"
)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
rehearsal_spec = importlib.util.spec_from_file_location(
    "backup_rehearsal", Path(__file__).resolve().parents[2] / "tools/package/browser-backup-rehearsal.py"
)
rehearsal = importlib.util.module_from_spec(rehearsal_spec)
rehearsal_spec.loader.exec_module(rehearsal)

POLICY = {
    "dataCompatibility": {
        "contract": "streamskope-browser-data-v1",
        "inspector": "dist/web/data-preflight.cjs",
        "reportSchemaVersion": 1,
    },
    "predecessors": [],
}


def release(version, digit):
    return {
        "version": version,
        "sourceRevision": digit * 40,
        "topologySha256": digit * 64,
        "manifestSha256": digit * 64,
        "reference": "ghcr.io/asadarafat/streamskope:" + version + "@sha256:" + digit * 64,
        "imageId": "sha256:" + digit * 64,
        "deliveryScope": "public-registry",
        "contract": POLICY["dataCompatibility"]["contract"],
        "inspector": True,
        "topology": "streamskope-" + version + ".clab.yml",
    }


def report(version):
    documents = [
        {"kind": kind, "state": "missing", "count": 0, "formats": [], "reason": None}
        for kind in m.DOCUMENTS
    ]
    documents[0].update(state="verified", count=4)
    documents[1].update(state="verified", count=1, formats=[1])
    return {
        "schemaVersion": 1,
        "dataContract": POLICY["dataCompatibility"]["contract"],
        "hostRelease": "v" + version,
        "outcome": "eligible",
        "documents": documents,
        "unverified": m.LIMITATIONS,
    }


class Crash(BaseException):
    pass


class Fixture:
    def __init__(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="streamskope-maintenance-core-")
        self.root = Path(self.temporary.name)
        self.uid, self.gid = os.getuid(), os.getgid()
        self.owner = self.uid or 1000
        self.data = self.root / "streamskope-data"
        self.data.mkdir(mode=0o700)
        os.chown(self.data, self.owner, self.gid)
        for name, content in (
            ("vault.lock", b""),
            ("vault.json", b"protected vault"),
            ("nats-profiles.json", b"protected profiles"),
        ):
            self.write(name, content)
        self.source, self.target = release("0.10.3", "a"), release("0.11.0", "b")
        self.state = {
            "schemaVersion": 1,
            **self.pointer(self.source),
            "uid": self.owner,
            "gid": self.gid,
            "home": str(self.root),
            "operatorUid": self.uid,
            "port": 18081,
        }
        m.atomic_json(self.root / "installation.json", self.state, self.uid, self.gid)
        stage = self.root / ".install-fixture"
        stage.mkdir(mode=0o700)
        self.config = {
            "root": str(self.root),
            "stateUid": self.uid,
            "stateGid": self.gid,
            "arch": "arm64",
            "lab": "fixture",
            "network": "fixture-mgmt",
            "container": "clab-fixture-app",
            "stage": str(stage),
            "target": self.pointer(self.target),
        }
        self.active = {
            "Id": "1" * 64,
            "RestartCount": 0,
            "State": {
                "StartedAt": "2026-10-08T00:00:00Z",
                "Running": True,
                "Status": "running",
                "ExitCode": 0,
                "OOMKilled": False,
            },
        }
        self.calls = []
        self.engine = None

    @staticmethod
    def pointer(value):
        return {key: value[key] for key in m.POINTER_KEYS}

    def write(self, name, content):
        path = self.data / name
        path.write_bytes(content)
        os.chmod(path, 0o600)
        os.chown(path, self.owner, self.gid)

    def open(self):
        if self.engine:
            self.engine.close()
        engine = m.Maintenance(self.config, POLICY)
        self.engine = engine
        engine.operator = lambda: None
        engine.release = lambda pointer: copy.deepcopy(
            next(value for value in (self.source, self.target) if self.pointer(value) == pointer)
        )
        engine.named_container = lambda: None if self.active is None else self.active["Id"]
        engine.conflicts = lambda _: None

        def owned(_release, expected=None):
            m.require(
                self.active is not None and (expected is None or self.active["Id"] == expected),
                "ownership-unconfirmed",
            )
            return copy.deepcopy(self.active)

        engine.owned = owned

        def preflight(inspector, data=None, *, target=None):
            m.require(not (self.data / "managed").exists(), "preflight-blocked")
            return report(inspector["version"])

        engine.preflight = preflight

        def docker(*args, **_options):
            self.calls.append(args)
            if args[0] == "stop":
                self.active["State"].update(Running=False, Status="exited")
            elif args[0] == "rm":
                self.active = None
            return 0, b""

        engine.docker = docker

        def deploy(target):
            self.calls.append(("deploy",))
            self.active = {
                "Id": "2" * 64,
                "RestartCount": 0,
                "State": {
                    "StartedAt": "2026-10-08T00:01:00Z",
                    "Running": True,
                    "Status": "running",
                    "ExitCode": 0,
                    "OOMKilled": False,
                },
            }
            engine.journal["candidateId"] = self.active["Id"]
            engine.save_journal("deployed")

        engine.deploy = deploy
        engine.ready = lambda: None
        return engine

    def interrupt(self, phase):
        engine = self.open()
        original = engine.save_journal

        def save(next_phase=None):
            original(next_phase)
            if engine.journal["phase"] == phase:
                raise Crash()

        engine.save_journal = save
        with unittest.TestCase().assertRaises(Crash):
            engine.execute("upgrade")
        engine.close()

    def close(self):
        if self.engine:
            self.engine.close()
        self.temporary.cleanup()


class TransactionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = Fixture()

    def tearDown(self):
        self.fixture.close()

    def recovery_fixture(self):
        f = self.fixture
        (f.root / "installer.lock").write_bytes(b"")
        os.chmod(f.root / "installer.lock", 0o600)
        queries = f.data / "queries"
        queries.mkdir(mode=0o700)
        os.chown(queries, f.owner, f.gid)
        f.write("queries/kafka-queries.json", b'{"schemaVersion":1,"queries":[]}\n')
        backup = f.root / "full-backup"
        backup.mkdir(mode=0o700)
        shutil.copytree(f.data, backup / "data")
        for path in [backup / "data", *(backup / "data").rglob("*")]:
            os.chown(path, f.owner, f.gid)
        entries = m.inventory(backup / "data", f.owner, f.gid)
        manifest = m.canonical({"entries": entries, "dataSnapshotSha256": m.digest(m.canonical(entries))})
        (backup / "inventory.json").write_bytes(manifest)
        os.chmod(backup / "inventory.json", 0o600)
        f.write("queries/kafka-queries.json", b'{"schemaVersion":2,"queries":[]}\n')
        f.write("queries/kafka-queries.json.pre-views-v1", b'{"schemaVersion":1,"queries":[]}\n')
        f.write("nats-profiles.json", b"newer protected profiles")
        return backup, m.digest(manifest), entries

    def test_explicit_operator_restore_preserves_changed_full_tree_and_original_lease_inode(self):
        f = self.fixture
        backup, digest, expected = self.recovery_fixture()
        before = m.inventory(f.data, f.owner, f.gid)
        inode = (f.data / "vault.lock").stat().st_ino
        preserved = f.root / "changed-data"
        result = rehearsal.restore(f.data, backup, preserved, f.owner, f.gid, digest)
        self.assertEqual(m.inventory(f.data, f.owner, f.gid), expected)
        self.assertEqual(m.inventory(preserved, f.owner, f.gid), before)
        self.assertEqual((f.data / "vault.lock").stat().st_ino, inode)
        self.assertTrue(result["originalLeaseInodePreserved"])
        self.assertTrue(result["originalInstallerLockInodePreserved"])
        self.assertEqual((f.data / "nats-profiles.json").read_bytes(), b"protected profiles")
        self.assertEqual((preserved / "nats-profiles.json").read_bytes(), b"newer protected profiles")
        self.assertEqual(result["backupInventorySha256"], digest)

    def test_operator_restore_refuses_changed_backup_before_preserving_or_mutating_live_tree(self):
        f = self.fixture
        backup, digest, _ = self.recovery_fixture()
        (backup / "data/nats-profiles.json").write_bytes(b"changed after backup qualification")
        before = m.inventory(f.data, f.owner, f.gid)
        preserved = f.root / "changed-data"
        with self.assertRaises(rehearsal.m.Refused):
            rehearsal.restore(f.data, backup, preserved, f.owner, f.gid, digest)
        self.assertFalse(preserved.exists())
        self.assertEqual(m.inventory(f.data, f.owner, f.gid), before)

    def test_operator_restore_cannot_replace_data_while_original_lease_is_held(self):
        f = self.fixture
        backup, digest, _ = self.recovery_fixture()
        before = m.inventory(f.data, f.owner, f.gid)
        preserved = f.root / "changed-data"
        with (f.data / "vault.lock").open("rb") as lease:
            m.fcntl.flock(lease, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                rehearsal.restore(f.data, backup, preserved, f.owner, f.gid, digest)
        self.assertFalse(preserved.exists())
        self.assertEqual(m.inventory(f.data, f.owner, f.gid), before)

    def test_operator_restore_refuses_an_active_installer_without_changing_data(self):
        f = self.fixture
        backup, digest, _ = self.recovery_fixture()
        before = m.inventory(f.data, f.owner, f.gid)
        preserved = f.root / "changed-data"
        with (f.root / "installer.lock").open("rb") as owner:
            m.fcntl.flock(owner, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                rehearsal.restore(f.data, backup, preserved, f.owner, f.gid, digest)
        self.assertFalse(preserved.exists())
        self.assertEqual(m.inventory(f.data, f.owner, f.gid), before)

    def test_current_codec_preferences_do_not_authorize_legacy_rollback(self):
        value = report("0.11.0")
        row = next(row for row in value["documents"] if row["kind"] == "preferences")
        row.update(state="verified", count=2, formats=[1, 2])
        current = release("0.11.0", "b")
        legacy = {**release("0.10.3", "a"), "inspector": False}
        self.assertEqual(m.inspection(value, POLICY, "0.11.0", current), value)
        with self.assertRaises(m.Refused) as rejected:
            m.inspection(value, POLICY, "0.11.0", legacy)
        self.assertEqual(rejected.exception.reason, "preflight-blocked")
        row["formats"] = [1]
        self.assertEqual(m.inspection(value, POLICY, "0.11.0", legacy), value)

    def test_current_views_do_not_authorize_legacy_rollback(self):
        value = report("0.11.0")
        row = next(row for row in value["documents"] if row["kind"] == "queries")
        row.update(state="verified", count=1, formats=[1, 2])
        current = release("0.11.0", "b")
        legacy = {**release("0.10.3", "a"), "inspector": False}
        self.assertEqual(m.inspection(value, POLICY, "0.11.0", current), value)
        with self.assertRaises(m.Refused) as rejected:
            m.inspection(value, POLICY, "0.11.0", legacy)
        self.assertEqual(rejected.exception.reason, "preflight-blocked")
        row["formats"] = [1]
        self.assertEqual(m.inspection(value, POLICY, "0.11.0", legacy), value)
        row["formats"] = [3]
        with self.assertRaises(m.Refused):
            m.inspection(value, POLICY, "0.11.0", current)

    def test_legacy_query_sidecars_refuse_fallback_before_docker_or_data_mutation(self):
        f = self.fixture
        (f.data / "queries").mkdir(mode=0o700)
        baseline = b'{"schemaVersion":1,"queries":[]}\n'
        (f.data / "queries/kafka-queries.json").write_bytes(baseline)
        legacy = {**f.source, "inspector": False}
        engine = f.open()
        calls = list(f.calls)
        for suffix in ("", ".1", ".99"):
            path = f.data / ("queries/kafka-queries.json.pre-views-v1" + suffix)
            path.write_bytes(baseline)
            with self.assertRaises(m.Refused) as rejected:
                m.Maintenance.preflight(engine, f.target, target=legacy)
            self.assertEqual(rejected.exception.reason, "preflight-blocked")
            self.assertEqual(f.calls, calls)
            self.assertEqual(path.read_bytes(), baseline)
            self.assertEqual((f.data / "queries/kafka-queries.json").read_bytes(), baseline)
            path.unlink()

    def test_current_inspection_does_not_authorize_security_profiles_for_legacy_target(self):
        for kind in ("kafka-profiles", "profile-backups"):
            with self.subTest(kind=kind):
                value = report("0.11.0")
                row = next(row for row in value["documents"] if row["kind"] == kind)
                row.update(state="verified", count=1, formats=[4])
                current = release("0.11.0", "b")
                legacy = {**release("0.10.3", "a"), "inspector": False}
                self.assertEqual(m.inspection(value, POLICY, "0.11.0", current), value)
                with self.assertRaises(m.Refused) as rejected:
                    m.inspection(value, POLICY, "0.11.0", legacy)
                self.assertEqual(rejected.exception.reason, "preflight-blocked")
                row["formats"] = [3]
                self.assertEqual(m.inspection(value, POLICY, "0.11.0", legacy), value)

    def test_legacy_rollback_with_expanded_credentials_is_refused_before_stopping_host(self):
        f = self.fixture
        f.source["inspector"] = False
        f.open().execute("upgrade")
        engine = f.open()
        calls = list(f.calls)
        state = (f.root / "installation.json").read_bytes()

        def expanded(inspector, data=None, *, target=None):
            value = report(inspector["version"])
            value["documents"][2].update(state="verified", count=1, formats=[4])
            return m.inspection(value, POLICY, inspector["version"], target)

        engine.preflight = expanded
        with self.assertRaises(m.Refused) as rejected:
            engine.execute("rollback")
        self.assertEqual(rejected.exception.reason, "preflight-blocked")
        self.assertEqual(f.calls, calls)
        self.assertEqual((f.root / "installation.json").read_bytes(), state)
        self.assertTrue(f.active["State"]["Running"])
        self.assertFalse((f.root / "maintenance.json").exists())

    def test_local_named_image_requires_the_sealed_image_identity_without_pulling(self):
        f = self.fixture
        local = {
            "version": f.target["version"],
            "sourceRevision": f.target["sourceRevision"],
            "platform": "linux/arm64",
            "imageId": f.target["imageId"],
        }
        reference = "streamskope:" + f.target["version"]
        topology = (Path(__file__).resolve().parents[2] / "streamskope.clab.yml").read_text()
        topology = topology.replace("streamskope:0.0.0-dev", reference).encode()
        manifest = m.canonical(
            {
                "schemaVersion": 1,
                "deliveryScope": "local-staged",
                **local,
                "dataCompatibility": POLICY["dataCompatibility"],
                "topology": {"file": f.target["topology"], "sha256": m.digest(topology)},
            }
        )
        selected = f.pointer(f.target)
        for kind, contents in (("topology", topology), ("manifest", manifest)):
            path = Path(f.config["stage"]) / kind
            path.write_bytes(contents)
            path.chmod(0o600)
            local[kind] = {"path": str(path), "sha256": m.digest(contents)}
            selected[kind + "Sha256"] = m.digest(contents)
        engine = m.Maintenance(f.config, POLICY, local)
        f.engine = engine
        image = {
            "Id": f.target["imageId"],
            "Os": "linux",
            "Architecture": "arm64",
            "Config": {
                "Labels": {
                    "org.opencontainers.image.version": f.target["version"],
                    "org.opencontainers.image.revision": f.target["sourceRevision"],
                }
            },
        }
        calls = []

        def docker(*args, **_options):
            calls.append(args)
            self.assertEqual(args, ("image", "inspect", reference))
            return 0, m.canonical([image])

        engine.docker = docker
        resolved = engine.release(selected)
        self.assertEqual(resolved["reference"], reference)
        self.assertEqual(resolved["imageId"], local["imageId"])
        # A tag moving to a different image never becomes new authority.
        image["Id"] = "sha256:" + "c" * 64
        with self.assertRaisesRegex(m.Refused, "unsupported-target"):
            engine.release(selected)
        engine.docker = lambda *_args, **_options: (1, b"")
        with self.assertRaisesRegex(m.Refused, "unsupported-target"):
            engine.release(selected)
        self.assertEqual(calls, [("image", "inspect", reference)] * 2)

    def test_commit_holds_real_original_inode_lease_through_ready_and_record_switch(self):
        f = self.fixture
        before = m.inventory(f.data, f.owner, f.gid)
        inode = (f.data / "vault.lock").stat().st_ino
        engine = f.open()
        observations = []

        def contender():
            result = subprocess.run(
                ["flock", "-n", "-E", "75", str(f.data / "vault.lock"), "true"], capture_output=True
            )
            observations.append(result.returncode)

        engine.ready = contender
        atomic = m.atomic_json

        def write(path, value, uid, gid):
            if Path(path).name == "installation.json":
                contender()
            atomic(path, value, uid, gid)

        with patch.object(m, "atomic_json", write):
            result = engine.execute("upgrade")
        self.assertEqual(observations, [75, 75])
        self.assertEqual(result["outcome"], "committed")
        self.assertEqual(result["previous"]["version"], "0.10.3")
        self.assertEqual((f.data / "vault.lock").stat().st_ino, inode)
        self.assertEqual(m.inventory(f.data, f.owner, f.gid), before)
        generation = f.root / result["backup"]["path"]
        self.assertEqual(m.inventory(generation / "data", f.owner, f.gid), before)
        self.assertEqual(
            m.digest((generation / "inventory.json").read_bytes()),
            result["backup"]["inventorySha256"],
        )
        self.assertEqual(
            json.loads((generation / "transaction.json").read_bytes())["phase"], "committed"
        )
        self.assertFalse((f.root / "maintenance.json").exists())
        engine.close()
        self.assertEqual(
            subprocess.run(["flock", "-n", str(f.data / "vault.lock"), "true"]).returncode, 0
        )

    def test_completed_recover_is_idempotent_and_retains_previous_identity(self):
        f = self.fixture
        f.open().execute("upgrade")
        result = f.open().execute("recover")
        self.assertEqual(result["outcome"], "unchanged")
        self.assertEqual(result["previous"]["version"], "0.10.3")
        self.assertEqual(sum(call[0] == "deploy" for call in f.calls), 1)

    def test_read_current_preserves_schema_one_and_two_and_blocks_active_journal(self):
        f = self.fixture
        self.assertEqual(m.read_current(f.root, f.uid, f.gid)[0], "0.10.3")
        f.open().execute("upgrade")
        self.assertEqual(m.read_current(f.root, f.uid, f.gid)[0], "0.11.0")
        (f.root / "maintenance.json").write_text("unknown")
        with self.assertRaisesRegex(m.Refused, "recovery-required"):
            m.read_current(f.root, f.uid, f.gid)

    def test_recover_intent_rechecks_managed_data_before_stopping(self):
        f = self.fixture
        f.interrupt("intent")
        f.write("managed", b"new managed profile")
        with self.assertRaisesRegex(m.Refused, "preflight-blocked"):
            f.open().execute("recover")
        self.assertEqual(f.calls, [])
        self.assertTrue(f.active["State"]["Running"])

    def test_recover_intent_rechecks_space_and_lease_before_stopping(self):
        f = self.fixture
        f.interrupt("intent")
        engine = f.open()
        engine.capacity = lambda _: (_ for _ in ()).throw(m.Refused("backup-unavailable"))
        with self.assertRaisesRegex(m.Refused, "backup-unavailable"):
            engine.execute("recover")
        self.assertEqual(f.calls, [])
        (f.data / "vault.lock").rename(f.data / "old.lock")
        f.write("vault.lock", b"")
        with self.assertRaisesRegex(m.Refused, "ownership-unconfirmed"):
            f.open().execute("recover")
        self.assertEqual(f.calls, [])

    def test_recover_stopped_completes_without_duplicate_stop(self):
        f = self.fixture
        f.interrupt("stopped")
        result = f.open().execute("recover")
        self.assertEqual(result["outcome"], "recovered")
        self.assertEqual(sum(call[0] == "stop" for call in f.calls), 1)

    def test_recover_retired_completes_without_removing_any_new_owner(self):
        f = self.fixture
        f.interrupt("retired")
        result = f.open().execute("recover")
        self.assertEqual(result["outcome"], "recovered")
        self.assertEqual(sum(call[0] == "rm" for call in f.calls), 1)

    def test_recover_deployed_refuses_changed_data_instead_of_restoring_snapshot(self):
        f = self.fixture
        f.interrupt("deployed")
        f.write("nats-profiles.json", b"newer user data")
        before = (f.root / "installation.json").read_bytes()
        with self.assertRaisesRegex(m.Refused, "recovery-required"):
            f.open().execute("recover")
        self.assertEqual((f.data / "nats-profiles.json").read_bytes(), b"newer user data")
        self.assertEqual((f.root / "installation.json").read_bytes(), before)
        self.assertTrue(f.active["State"]["Running"])

    def test_complete_record_stale_journal_only_archives_without_network_or_data_checks(self):
        f = self.fixture
        engine = f.open()
        engine.archive = lambda: (_ for _ in ()).throw(Crash())
        with self.assertRaises(Crash):
            engine.execute("upgrade")
        engine.close()
        f.write("nats-profiles.json", b"legitimate postcommit edits")
        engine = f.open()
        engine.release = lambda _: (_ for _ in ()).throw(
            AssertionError("network must not be needed")
        )
        result = engine.execute("recover")
        self.assertEqual(result["outcome"], "recovered")
        self.assertEqual(
            (f.data / "nats-profiles.json").read_bytes(), b"legitimate postcommit edits"
        )
        self.assertFalse((f.root / "maintenance.json").exists())

    def test_partial_backup_is_preserved_while_recovery_creates_complete_next_attempt(self):
        f = self.fixture
        with patch.object(m.shutil, "copyfileobj", side_effect=OSError("disk interrupted")):
            with self.assertRaises(OSError):
                f.open().execute("upgrade")
        result = f.open().execute("recover")
        transaction = f.root / "backups" / result["transactionId"]
        self.assertEqual(
            sorted(path.name for path in transaction.iterdir()), ["attempt-1", "attempt-2"]
        )
        self.assertTrue((transaction / "attempt-1/data").exists())
        self.assertTrue((transaction / "attempt-2/inventory.json").exists())

    def test_ambiguous_candidate_without_recorded_id_is_preserved(self):
        f = self.fixture
        f.interrupt("retired")
        f.active = {"Id": "f" * 64, "State": {"Running": True}}
        before = list(f.calls)
        with self.assertRaisesRegex(m.Refused, "recovery-required"):
            f.open().execute("recover")
        self.assertEqual(f.calls, before)
        self.assertEqual(f.active["Id"], "f" * 64)

    def test_held_original_vault_lease_refuses_recovery_without_deployment(self):
        f = self.fixture
        f.interrupt("stopped")
        with open(f.data / "vault.lock", "rb") as owner:
            m.fcntl.flock(owner, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
            with self.assertRaisesRegex(m.Refused, "lease-held"):
                f.open().execute("recover")
        self.assertFalse(any(call[0] == "deploy" for call in f.calls))

    def test_candidate_fresh_setup_is_not_ready(self):
        engine = self.fixture.open()
        engine.run = lambda *args, **kwargs: (
            0,
            b'{"status":"locked","state":"locked","setupRequired":true}',
        )
        with patch.object(m.time, "sleep"):
            with self.assertRaisesRegex(m.Refused, "candidate-unavailable"):
                m.Maintenance.ready(engine)

    def test_candidate_stopping_during_readiness_never_switches_record(self):
        f = self.fixture
        engine = f.open()
        engine.ready = lambda: f.active["State"].update(Running=False, Status="exited")
        original = (f.root / "installation.json").read_bytes()
        with self.assertRaisesRegex(m.Refused, "candidate-unavailable"):
            engine.execute("upgrade")
        self.assertEqual((f.root / "installation.json").read_bytes(), original)
        self.assertTrue((f.root / "maintenance.json").exists())

    def test_inspector_timeout_only_removes_its_verified_readonly_worker(self):
        for foreign in (False, True):
            with self.subTest(foreign=foreign):
                f = self.fixture
                engine = f.open()
                retained = None
                removed = []

                def docker(*args, **options):
                    nonlocal retained
                    if args[0] == "run":
                        name = args[args.index("--name") + 1]
                        nonce = args[args.index("--label") + 1].split("=", 1)[1]
                        retained = {
                            "Id": "e" * 64,
                            "Name": "/" + name,
                            "Image": f.target["imageId"],
                            "Config": {
                                "Image": f.target["imageId"],
                                "Labels": {
                                    "io.streamskope.maintenance": "foreign" if foreign else nonce
                                },
                                "User": str(f.owner) + ":" + str(f.gid),
                                "Entrypoint": ["node"],
                                "Cmd": [POLICY["dataCompatibility"]["inspector"], "/data"],
                            },
                            "HostConfig": {
                                "ReadonlyRootfs": True,
                                "NetworkMode": "none",
                                "Privileged": False,
                                "CapAdd": None,
                                "CapDrop": ["ALL"],
                                "SecurityOpt": ["no-new-privileges:true"],
                            },
                            "Mounts": [
                                {
                                    "Type": "bind",
                                    "Source": str(f.data),
                                    "Destination": "/data",
                                    "RW": False,
                                }
                            ],
                        }
                        raise m.Refused("candidate-unavailable")
                    if args[:2] == ("container", "ls"):
                        return 0, b"" if retained is None else (retained["Id"] + "\n").encode()
                    if args[:2] == ("container", "inspect"):
                        return 0, m.canonical([retained])
                    self.assertEqual(args, ("rm", "--force", "e" * 64))
                    removed.append(args)
                    retained = None
                    return 0, b""

                engine.docker = docker
                with self.assertRaisesRegex(
                    m.Refused, "cleanup-unconfirmed" if foreign else "candidate-unavailable"
                ):
                    m.Maintenance.preflight(engine, f.target)
                self.assertEqual(len(removed), 0 if foreign else 1)
                self.assertEqual(f.active["Id"], "1" * 64)


class CommandBoundaryTest(unittest.TestCase):
    def invoke(self, script, timeout=5):
        processes = []
        original = subprocess.Popen

        def start(*args, **kwargs):
            process = original(*args, **kwargs)
            processes.append(process)
            return process

        with patch.object(m.subprocess, "Popen", start):
            with self.assertRaisesRegex(m.Refused, "^candidate-unavailable$"):
                m.command([sys.executable, "-c", script], timeout=timeout, allow_failure=True)
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].returncode)
        with self.assertRaises(ChildProcessError):
            os.waitpid(processes[0].pid, os.WNOHANG)

    def test_live_stdout_and_stderr_are_each_stopped_at_the_shared_bound(self):
        for stream in (1, 2):
            with self.subTest(stream=stream), patch.object(m, "MAX_OUTPUT", 32 * 1024):
                self.invoke(
                    "import os,time; os.write("
                    + str(stream)
                    + ", b'private-output-sentinel' * 4096); time.sleep(60)"
                )

    def test_neither_stream_can_hide_under_a_separate_limit(self):
        with patch.object(m, "MAX_OUTPUT", 24 * 1024):
            self.invoke(
                "import os,time; os.write(1,b'x'*16000); os.write(2,b'y'*16000); time.sleep(60)"
            )

    def test_silent_timeout_kills_and_reaps_without_returning_command_text(self):
        self.invoke("import time; time.sleep(60)", timeout=0.1)

    def test_bounded_stderr_is_discarded_and_explicit_nonzero_result_is_retained(self):
        result = m.command(
            [
                sys.executable,
                "-c",
                "import os,sys; os.write(1,b'public'); os.write(2,b'private-output-sentinel'); sys.exit(7)",
            ],
            allow_failure=True,
        )
        self.assertEqual(result, (7, b"public"))


class BoundaryTest(unittest.TestCase):
    def test_semver_precedence_and_equal_identity_policy(self):
        ordered = [
            "0.11.0-alpha",
            "0.11.0-alpha.1",
            "0.11.0-alpha.2",
            "0.11.0-alpha.10",
            "0.11.0-beta",
            "0.11.0-rc.1",
            "0.11.0",
            "0.12.0",
            "1.0.0",
        ]
        for left, right in zip(ordered, ordered[1:]):
            self.assertTrue(m.newer(right, left))
            self.assertFalse(m.newer(left, right))
        self.assertFalse(m.newer("0.11.0", "0.11.0"))
        with self.assertRaises(m.Refused):
            m.newer("0.11.0-rc.01", "0.10.3")

    def test_initialized_vault_proof_required_beyond_generic_preflight_eligibility(self):
        value = report("0.11.0")
        value["documents"][1].update(state="missing", count=0, formats=[])
        with self.assertRaisesRegex(m.Refused, "preflight-blocked"):
            m.inspection(value, POLICY, "0.11.0")

    def test_journal_and_state_json_refuse_duplicate_keys(self):
        with self.assertRaises(m.Refused):
            m.decode(b'{"schemaVersion":1,"schemaVersion":2}')

    def test_private_inventory_refuses_links_without_following_or_repairing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.write_text("untouched")
            data = root / "data"
            data.mkdir(mode=0o700)
            (data / "secret").symlink_to(outside)
            with self.assertRaises(m.Refused):
                m.inventory(data, os.getuid(), os.getgid())
            self.assertEqual(outside.read_text(), "untouched")


if __name__ == "__main__":
    unittest.main(verbosity=2)
