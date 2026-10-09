"""Owned browser maintenance. Embedded unchanged in the published one-file installer.

The shell holds installer.lock. This module owns the data lease and the durable
transaction; an external command finishing never substitutes for its proof.
"""

import base64
import fcntl
import hashlib
import json
import os
import re
import selectors
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

POLICY_B64 = "@STREAMSKOPE_MAINTENANCE_POLICY_B64@"
LOCAL_B64 = "@STREAMSKOPE_MAINTENANCE_LOCAL_B64@"
MAX_OUTPUT = 2 * 1024 * 1024
HEX = re.compile(r"[a-f0-9]{64}")
VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?")
POINTER_KEYS = {"version", "sourceRevision", "topologySha256", "manifestSha256"}
OWNER_KEYS = {"uid", "gid", "home", "operatorUid", "port"}
PHASES = ("intent", "stopped", "backed-up", "retiring", "retired", "deployed", "committed")
REASONS = {
    "invalid-state",
    "unsupported-target",
    "ownership-unconfirmed",
    "lease-missing",
    "lease-held",
    "preflight-blocked",
    "cleanup-unconfirmed",
    "backup-unavailable",
    "recovery-required",
    "candidate-unavailable",
}
DOCUMENTS = (
    "filesystem",
    "vault",
    "kafka-profiles",
    "nats-profiles",
    "rules",
    "preferences",
    "topic-history",
    "queries",
    "trust-recipes",
    "observations",
    "plugin-installations",
    "plugin-network",
    "plugin-catalog",
    "plugin-package-cache",
    "plugin-recovery",
    "profile-backups",
    "host-state",
)
LIMITATIONS = [
    "protected-content-authenticity",
    "protected-profile-schema",
    "remote-plugin-resource-cleanup",
    "host-quiescence",
]


class Refused(Exception):
    def __init__(self, reason):
        self.reason = reason if reason in REASONS else "invalid-state"
        super().__init__(self.reason)


def require(condition, reason="invalid-state"):
    if not condition:
        raise Refused(reason)


def exact(value, keys):
    require(isinstance(value, dict) and set(value) == set(keys))
    return value


def canonical(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def integer(value, minimum=0, maximum=2**53 - 1):
    return type(value) is int and minimum <= value <= maximum


def pointer(value):
    exact(value, POINTER_KEYS)
    require(
        isinstance(value["version"], str)
        and len(value["version"]) <= 128
        and VERSION.fullmatch(value["version"])
    )
    require(
        isinstance(value["sourceRevision"], str)
        and re.fullmatch(r"[a-f0-9]{40}", value["sourceRevision"])
    )
    require(
        all(
            isinstance(value[key], str) and HEX.fullmatch(value[key])
            for key in ("topologySha256", "manifestSha256")
        )
    )
    return dict(value)


def installation(value):
    require(isinstance(value, dict) and type(value.get("schemaVersion")) is int)
    if value["schemaVersion"] == 1:
        exact(value, {"schemaVersion"} | POINTER_KEYS | OWNER_KEYS)
        current, previous = pointer({key: value[key] for key in POINTER_KEYS}), None
    else:
        exact(value, {"schemaVersion", "current", "previous"} | OWNER_KEYS)
        require(value["schemaVersion"] == 2)
        current = pointer(value["current"])
        previous = None if value["previous"] is None else pointer(value["previous"])
    require(integer(value["uid"], 1) and integer(value["gid"]) and integer(value["operatorUid"]))
    require(integer(value["port"], 1024, 65535))
    require(
        isinstance(value["home"], str)
        and value["home"].startswith("/")
        and len(value["home"]) <= 4096
        and not any(c in value["home"] for c in "\r\n\0")
    )
    return {
        "schemaVersion": 2,
        "current": current,
        "previous": previous,
        **{key: value[key] for key in OWNER_KEYS},
    }


def uuid_text(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def safe_ancestors(path):
    path = Path(path)
    require(path.is_absolute() and "\0" not in str(path) and len(str(path)) <= 4096)
    for item in reversed((path, *path.parents)):
        info = item.lstat()
        require(stat.S_ISDIR(info.st_mode), "ownership-unconfirmed")


def safe_directory(path, uid, gid, mode=0o700):
    safe_ancestors(path)
    info = Path(path).lstat()
    require(
        info.st_uid == uid and info.st_gid == gid and stat.S_IMODE(info.st_mode) == mode,
        "ownership-unconfirmed",
    )
    return info


def private_read(path, uid, gid, maximum=65536):
    safe_ancestors(Path(path).parent)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(
            stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1
            and info.st_uid == uid
            and info.st_gid == gid
            and stat.S_IMODE(info.st_mode) == 0o600
            and info.st_size <= maximum,
            "ownership-unconfirmed",
        )
        with os.fdopen(fd, "rb", closefd=False) as source:
            content = source.read(maximum + 1)
        require(len(content) <= maximum, "invalid-state")
        return content
    finally:
        os.close(fd)


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, value, uid, gid):
    path = Path(path)
    safe_directory(path.parent, uid, gid)
    if os.path.lexists(path):
        private_read(path, uid, gid, MAX_OUTPUT)
    fd, temporary = tempfile.mkstemp(prefix=".maintenance-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        os.fchown(fd, uid, gid)
        with os.fdopen(fd, "wb") as output:
            output.write(canonical(value))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def command(args, env=None, timeout=120, allow_failure=False):
    """Bound both live output streams; discard stderr and always reap the child."""
    process = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    deadline = time.monotonic() + timeout
    output, captured = [], 0
    try:
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            ready.register(process.stderr, selectors.EVENT_READ)
            while ready.get_map():
                remaining = deadline - time.monotonic()
                require(remaining > 0, "candidate-unavailable")
                for event, _ in ready.select(min(remaining, 0.1)):
                    chunk = os.read(event.fd, min(65536, MAX_OUTPUT - captured + 1))
                    if not chunk:
                        ready.unregister(event.fileobj)
                        continue
                    captured += len(chunk)
                    require(captured <= MAX_OUTPUT, "candidate-unavailable")
                    if event.fileobj is process.stdout:
                        output.append(chunk)
            remaining = deadline - time.monotonic()
            require(remaining > 0, "candidate-unavailable")
            try:
                code = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                raise Refused("candidate-unavailable") from None
        require(allow_failure or code == 0, "candidate-unavailable")
        return code, b"".join(output)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()
        process.stderr.close()


def inventory(root, uid, gid):
    """Canonical byte inventory; never follows links or accepts another owner's objects."""
    safe_directory(root, uid, gid)
    entries, total = [], 0

    def walk(relative, depth):
        nonlocal total
        require(depth <= 6 and len(entries) < 8192, "backup-unavailable")
        path = Path(root) / relative
        info = path.lstat()
        require(
            info.st_uid == uid
            and info.st_gid == gid
            and stat.S_IMODE(info.st_mode) in (0o600, 0o700),
            "ownership-unconfirmed",
        )
        entry = {
            "path": relative,
            "mode": stat.S_IMODE(info.st_mode),
            "uid": uid,
            "gid": gid,
            "bytes": 0,
            "sha256": None,
        }
        if stat.S_ISDIR(info.st_mode):
            require(entry["mode"] == 0o700, "ownership-unconfirmed")
            entry["type"] = "directory"
            entries.append(entry)
            for child in sorted(os.listdir(path)):
                walk(str(Path(relative) / child), depth + 1)
        else:
            require(
                stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1
                and entry["mode"] == 0o600
                and info.st_size <= 64 * 1024 * 1024,
                "ownership-unconfirmed",
            )
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                opened = os.fstat(fd)
                require(
                    (opened.st_dev, opened.st_ino, opened.st_size)
                    == (info.st_dev, info.st_ino, info.st_size),
                    "ownership-unconfirmed",
                )
                hashed, length = hashlib.sha256(), 0
                while True:
                    chunk = os.read(fd, 1024 * 1024)
                    if not chunk:
                        break
                    length += len(chunk)
                    require(length <= info.st_size, "backup-unavailable")
                    hashed.update(chunk)
                require(
                    length == info.st_size and os.fstat(fd).st_mtime_ns == info.st_mtime_ns,
                    "backup-unavailable",
                )
            finally:
                os.close(fd)
            total += length
            require(total <= 16 * 1024**3, "backup-unavailable")
            entry.update(type="file", bytes=length, sha256=hashed.hexdigest())
            entries.append(entry)

    walk("", 0)
    return sorted(entries, key=lambda entry: entry["path"])


def inspection(value, policy, version, target=None):
    # Legacy targets have no inspector. A current inspector accepting format 4
    # cannot establish that those older binaries can read the expanded credentials.
    legacy_target = target is not None and not target["inspector"]
    kafka_maximum = 3 if legacy_target else 4
    preference_maximum = 1 if legacy_target else 2
    query_maximum = 1 if legacy_target else 3
    exact(
        value,
        {"schemaVersion", "dataContract", "hostRelease", "outcome", "documents", "unverified"},
    )
    require(
        value["schemaVersion"] == 1
        and type(value["schemaVersion"]) is int
        and value["dataContract"] == policy["dataCompatibility"]["contract"]
        and value["hostRelease"] == "v" + version
        and value["outcome"] == "eligible"
        and value["unverified"] == LIMITATIONS,
        "preflight-blocked",
    )
    require(
        isinstance(value["documents"], list) and len(value["documents"]) == len(DOCUMENTS),
        "preflight-blocked",
    )
    for kind, row in zip(DOCUMENTS, value["documents"]):
        exact(row, {"kind", "state", "count", "formats", "reason"})
        require(
            row["kind"] == kind
            and row["state"] in ("verified", "missing")
            and row["reason"] is None
            and integer(row["count"], 0, 8192),
            "preflight-blocked",
        )
        formats = row["formats"]
        require(
            isinstance(formats, list)
            and all(
                integer(number, 1, kafka_maximum if kind in ("kafka-profiles", "profile-backups") else preference_maximum if kind == "preferences" else query_maximum if kind == "queries" else 1)
                for number in formats
            )
            and formats == sorted(set(formats)),
            "preflight-blocked",
        )
        require(
            row["state"] != "missing" or (row["count"] == 0 and formats == []), "preflight-blocked"
        )
    require(
        value["documents"][0]["state"] == "verified"
        and value["documents"][1]["state"] == "verified"
        and value["documents"][1]["count"] == 1
        and value["documents"][1]["formats"] == [1],
        "preflight-blocked",
    )
    return value


def decode(content):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result)
            result[key] = value
        return result

    return json.loads(content, object_pairs_hook=unique)


class Maintenance:
    def __init__(self, config, policy, local=None, run=command):
        self.config, self.policy, self.local, self.run = config, policy, local, run
        self.root = Path(config["root"])
        self.stage = Path(config["stage"])
        self.uid, self.gid = config["stateUid"], config["stateGid"]
        self.data = self.root / "streamskope-data"
        self.state_path = self.root / "installation.json"
        self.journal_path = self.root / "maintenance.json"
        self.transaction_id = None
        self.journal = None
        self.lease = None
        self.images = {}
        self.environment = {
            **os.environ,
            "DOCKER_HOST": "unix:///var/run/docker.sock",
            "DOCKER_CONTEXT": "default",
        }
        safe_directory(self.root, self.uid, self.gid)
        safe_directory(self.stage, self.uid, self.gid)
        require(self.stage.parent == self.root and self.stage.name.startswith(".install-"))
        self.original_bytes = private_read(self.state_path, self.uid, self.gid)
        self.original = decode(self.original_bytes)
        self.state = installation(self.original)
        safe_directory(self.data, self.state["uid"], self.state["gid"])
        exact(policy, {"dataCompatibility", "predecessors"})
        require(
            policy["dataCompatibility"]
            == {
                "contract": "streamskope-browser-data-v1",
                "inspector": "dist/web/data-preflight.cjs",
                "reportSchemaVersion": 1,
            }
        )
        require(isinstance(policy["predecessors"], list))
        self.origin = "http://127.0.0.1:" + str(self.state["port"])

    def docker(self, *args, allow_failure=False, timeout=120):
        return self.run(
            ["docker", "--host", "unix:///var/run/docker.sock", *args],
            env=self.environment,
            timeout=timeout,
            allow_failure=allow_failure,
        )

    def json_docker(self, *args):
        return decode(self.docker(*args)[1])

    def operator(self):
        value = os.environ.get("SUDO_UID", "0")
        require(
            re.fullmatch(r"0|[1-9][0-9]*", value) and int(value) == self.state["operatorUid"],
            "ownership-unconfirmed",
        )
        selector = value if value != "0" else "streamskope-browser"
        raw = self.run(["getent", "passwd", selector], timeout=10)[1].decode().strip().split(":")
        require(
            len(raw) == 7
            and raw[2:4] == [str(self.state["uid"]), str(self.state["gid"])]
            and raw[5] == self.state["home"],
            "ownership-unconfirmed",
        )
        if value == "0":
            require(
                raw[0] == "streamskope-browser"
                and raw[5] == str(self.root)
                and raw[6] == "/usr/sbin/nologin",
                "ownership-unconfirmed",
            )

    def asset(self, name, version, expected=None):
        path = self.root / name
        if path.exists() or path.is_symlink():
            contents = private_read(path, self.uid, self.gid, 1024 * 1024)
        else:
            temporary = self.stage / ("asset-" + str(uuid.uuid4()))
            self.run(
                [
                    "curl",
                    "--proto",
                    "=https",
                    "--proto-redir",
                    "=https",
                    "--tlsv1.2",
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--connect-timeout",
                    "15",
                    "--max-time",
                    "120",
                    "--max-filesize",
                    "1048576",
                    "--output",
                    str(temporary),
                    "https://github.com/asadarafat/streamskope/releases/download/v"
                    + version
                    + "/"
                    + ("SHA256SUMS" if name.startswith("SHA256SUMS-") else name),
                ],
                env=self.environment,
            )
            os.chmod(temporary, 0o600)
            os.chown(temporary, self.uid, self.gid)
            contents = private_read(temporary, self.uid, self.gid, 1024 * 1024)
            require(expected is None or digest(contents) == expected, "unsupported-target")
            # The installer lock owns these immutable cache names; never replace an existing file.
            require(not os.path.lexists(path), "invalid-state")
            with open(temporary, "rb") as stream:
                os.fsync(stream.fileno())
            os.rename(temporary, path)
            fsync_directory(self.root)
        require(expected is None or digest(contents) == expected, "unsupported-target")
        return contents

    def local_release(self, selected):
        if (
            self.local is None
            or self.local.get("version") != selected["version"]
            or self.local.get("sourceRevision") != selected["sourceRevision"]
        ):
            return None
        exact(
            self.local, {"version", "sourceRevision", "platform", "imageId", "manifest", "topology"}
        )
        require(
            self.local["platform"] == "linux/" + self.config["arch"]
            and re.fullmatch(r"sha256:[a-f0-9]{64}", self.local["imageId"]),
            "unsupported-target",
        )
        inputs = {}
        for field, key in (("manifest", "manifestSha256"), ("topology", "topologySha256")):
            descriptor = exact(self.local[field], {"path", "sha256"})
            require(
                descriptor["sha256"] == selected[key] and Path(descriptor["path"]).is_absolute(),
                "unsupported-target",
            )
            contents = private_read(descriptor["path"], self.uid, self.gid, 1024 * 1024)
            require(digest(contents) == descriptor["sha256"], "unsupported-target")
            inputs[field] = contents
        value = decode(inputs["manifest"])
        exact(
            value,
            {
                "schemaVersion",
                "deliveryScope",
                "version",
                "sourceRevision",
                "platform",
                "imageId",
                "dataCompatibility",
                "topology",
            },
        )
        require(
            value
            == {
                "schemaVersion": 1,
                "deliveryScope": "local-staged",
                "version": selected["version"],
                "sourceRevision": selected["sourceRevision"],
                "platform": self.local["platform"],
                "imageId": self.local["imageId"],
                "dataCompatibility": self.policy["dataCompatibility"],
                "topology": {
                    "file": "streamskope-" + selected["version"] + ".clab.yml",
                    "sha256": selected["topologySha256"],
                },
            },
            "unsupported-target",
        )
        topology_name = value["topology"]["file"]
        self.cache_exact(topology_name, inputs["topology"])
        self.cache_exact(
            "streamskope-" + selected["version"] + "-container.json", inputs["manifest"]
        )
        return {
            **selected,
            # Containerlab resolves named images before creation; a bare Docker ID
            # is not accepted there. The closed local tag is still independently
            # inspected against this sealed image ID before and after deployment.
            "reference": "streamskope:" + selected["version"],
            "imageId": value["imageId"],
            "deliveryScope": "local-staged",
            "contract": value["dataCompatibility"]["contract"],
            "inspector": True,
            "topology": topology_name,
        }

    def cache_exact(self, name, content):
        path = self.root / name
        if os.path.lexists(path):
            require(
                private_read(path, self.uid, self.gid, 1024 * 1024) == content, "unsupported-target"
            )
            return
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.fchown(fd, self.uid, self.gid)
            with os.fdopen(fd, "wb", closefd=False) as output:
                output.write(content)
                output.flush()
                os.fsync(fd)
        finally:
            os.close(fd)
        fsync_directory(self.root)

    def release(self, selected):
        selected = pointer(selected)
        resolved = self.local_release(selected)
        if resolved is None:
            version = selected["version"]
            topology_name, manifest_name = (
                "streamskope-" + version + ".clab.yml",
                "streamskope-" + version + "-container.json",
            )
            checks = self.asset(
                "SHA256SUMS-" + version + "-" + selected["sourceRevision"], version
            ).decode()
            sums = {}
            for line in checks.splitlines():
                match = re.fullmatch(r"([a-f0-9]{64}) [ *]([^/\\\0]+)", line)
                require(match and match[2] not in sums, "unsupported-target")
                sums[match[2]] = match[1]
            require(
                sums.get(topology_name) == selected["topologySha256"]
                and sums.get(manifest_name) == selected["manifestSha256"],
                "unsupported-target",
            )
            value = decode(self.asset(manifest_name, version, selected["manifestSha256"]))
            self.asset(topology_name, version, selected["topologySha256"])
            require(
                value.get("schemaVersion") in (2, 3, 4)
                and type(value["schemaVersion"]) is int
                and value.get("version") == version
                and value.get("sourceRevision") == selected["sourceRevision"]
                and value.get("format") == "docker-save-gzip"
                and value.get("image") == "streamskope:" + version
                and value.get("topology")
                == {"file": topology_name, "sha256": selected["topologySha256"]},
                "unsupported-target",
            )
            registry = value["registry"]
            exact(
                registry,
                {
                    "schemaVersion",
                    "version",
                    "sourceRevision",
                    "image",
                    "digest",
                    "reference",
                    "platforms",
                },
            )
            require(
                registry["schemaVersion"] == 1
                and registry["version"] == version
                and registry["sourceRevision"] == selected["sourceRevision"]
                and registry["image"] == "ghcr.io/asadarafat/streamskope:" + version
                and re.fullmatch(r"sha256:[a-f0-9]{64}", registry["digest"])
                and registry["reference"] == registry["image"] + "@" + registry["digest"],
                "unsupported-target",
            )
            require(
                isinstance(registry["platforms"], list)
                and [item["platform"] for item in registry["platforms"]]
                == ["linux/amd64", "linux/arm64"],
                "unsupported-target",
            )
            for item in registry["platforms"]:
                exact(item, {"platform", "manifestDigest", "imageId"})
                require(
                    all(
                        re.fullmatch(r"sha256:[a-f0-9]{64}", item[key])
                        for key in ("manifestDigest", "imageId")
                    ),
                    "unsupported-target",
                )
            platform = next(
                item
                for item in registry["platforms"]
                if item["platform"] == "linux/" + self.config["arch"]
            )
            contract, has_inspector = None, value["schemaVersion"] == 4
            if has_inspector:
                require(
                    value.get("dataCompatibility") == self.policy["dataCompatibility"],
                    "unsupported-target",
                )
                contract = self.policy["dataCompatibility"]["contract"]
            else:
                for old in self.policy["predecessors"]:
                    if (
                        old["version"] == version
                        and old["sourceRevision"] == selected["sourceRevision"]
                        and old["registryReference"] == registry["reference"]
                        and old["images"].get(self.config["arch"]) == platform["imageId"]
                    ):
                        contract = old["contract"]
            require(contract == self.policy["dataCompatibility"]["contract"], "unsupported-target")
            resolved = {
                **selected,
                "reference": registry["reference"],
                "imageId": platform["imageId"],
                "deliveryScope": "public-registry",
                "contract": contract,
                "inspector": has_inspector,
                "topology": topology_name,
            }
        text = private_read(
            self.root / resolved["topology"], self.uid, self.gid, 1024 * 1024
        ).decode()
        for expected in (
            "name: streamskope",
            "image: ${STREAMSKOPE_IMAGE:=" + resolved["reference"] + "}",
            "image-pull-policy: "
            + ("Never" if resolved["deliveryScope"] == "local-staged" else "IfNotPresent"),
            "./streamskope-data:/data",
            "${STREAMSKOPE_UID:=1000}:${STREAMSKOPE_GID:=1000}",
            "${STREAMSKOPE_HOST_BIND:=127.0.0.1}:${STREAMSKOPE_HOST_PORT:=8080}:8080/tcp",
            "${STREAMSKOPE_PUBLIC_ORIGIN:=http://127.0.0.1:8080}",
            "privileged: false",
            "no-new-privileges:true",
            "cap-add: []",
        ):
            require(text.count(expected) == 1, "unsupported-target")
        code, content = self.docker("image", "inspect", resolved["reference"], allow_failure=True)
        if code:
            require(resolved["deliveryScope"] == "public-registry", "unsupported-target")
            self.docker("pull", resolved["reference"], timeout=600)
            content = self.docker("image", "inspect", resolved["reference"])[1]
        images = decode(content)
        require(isinstance(images, list) and len(images) == 1, "unsupported-target")
        image = images[0]
        labels = image["Config"]["Labels"]
        require(
            image["Id"] == resolved["imageId"]
            and image["Os"] == "linux"
            and image["Architecture"] == self.config["arch"]
            and labels.get("org.opencontainers.image.version") == resolved["version"]
            and labels.get("org.opencontainers.image.revision") == resolved["sourceRevision"],
            "unsupported-target",
        )
        self.images[resolved["imageId"]] = image
        return resolved

    @staticmethod
    def identity(release):
        return {
            key: release[key]
            for key in ("version", "sourceRevision", "reference", "imageId", "deliveryScope")
        }

    def named_container(self):
        identifiers = (
            self.docker(
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                "name=^/" + self.config["container"] + "$",
                "--format",
                "{{.ID}}",
            )[1]
            .decode()
            .splitlines()
        )
        require(
            len(identifiers) <= 1 and all(HEX.fullmatch(item) for item in identifiers),
            "ownership-unconfirmed",
        )
        return None if not identifiers else identifiers[0]

    def owned(self, release, expected_id=None):
        identifier = self.named_container()
        require(
            identifier is not None and (expected_id is None or identifier == expected_id),
            "ownership-unconfirmed",
        )
        values = self.json_docker("container", "inspect", identifier)
        require(isinstance(values, list) and len(values) == 1, "ownership-unconfirmed")
        value = values[0]
        config, host = value["Config"], value["HostConfig"]
        image_config = self.images[release["imageId"]]["Config"]
        labels = config["Labels"]
        require(
            value["Id"] == identifier
            and value["Name"] == "/" + self.config["container"]
            and value["Image"] == release["imageId"]
            and config["Image"] == release["reference"]
            and config["User"] == str(self.state["uid"]) + ":" + str(self.state["gid"]),
            "ownership-unconfirmed",
        )
        require(
            config.get("Entrypoint") == image_config.get("Entrypoint")
            and config.get("Cmd") == image_config.get("Cmd"),
            "ownership-unconfirmed",
        )
        require(
            labels.get("containerlab") == self.config["lab"]
            and labels.get("clab-topo-file") == str(self.root / release["topology"])
            and labels.get("io.streamskope.deployment") == "browser",
            "ownership-unconfirmed",
        )
        require(
            host.get("Privileged") is False
            and not host.get("CapAdd")
            and host.get("RestartPolicy", {}).get("Name") == "unless-stopped"
            and host.get("SecurityOpt") in (["no-new-privileges"], ["no-new-privileges:true"]),
            "ownership-unconfirmed",
        )
        require(
            host.get("PortBindings")
            == {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(self.state["port"])}]},
            "ownership-unconfirmed",
        )
        require(
            [item for item in config["Env"] if item.startswith("STREAMSKOPE_PUBLIC_ORIGIN=")]
            == ["STREAMSKOPE_PUBLIC_ORIGIN=" + self.origin],
            "ownership-unconfirmed",
        )
        for key in (
            "NODE_OPTIONS",
            "STREAMSKOPE_DATA_DIR",
            "STREAMSKOPE_DATA_ROOT",
            "STREAMSKOPE_BROWSER_HOST",
        ):
            require(
                [item for item in config["Env"] if item.startswith(key + "=")]
                == [item for item in image_config.get("Env", []) if item.startswith(key + "=")],
                "ownership-unconfirmed",
            )
        mounts = [item for item in value["Mounts"] if item["Destination"] == "/data"]
        require(
            len(mounts) == 1
            and mounts[0]["Type"] == "bind"
            and mounts[0]["Source"] == str(self.data)
            and mounts[0]["RW"] is True,
            "ownership-unconfirmed",
        )
        # Containerlab may add its generated hosts file; no arbitrary extra writable bind.
        for item in value["Mounts"]:
            if item["Destination"] == "/data":
                continue
            require(
                item["Type"] == "bind"
                and item["Destination"] == "/etc/hosts"
                and (self.root / ("clab-" + self.config["lab"])) in Path(item["Source"]).parents,
                "ownership-unconfirmed",
            )
        require(
            not any(value["State"].get(key, False) for key in ("Paused", "Restarting", "Dead")),
            "ownership-unconfirmed",
        )
        return value

    def conflicts(self, allowed_id):
        ids = (
            self.docker("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}")[1]
            .decode()
            .splitlines()
        )
        require(
            len(ids) <= 8192 and all(HEX.fullmatch(item) for item in ids), "ownership-unconfirmed"
        )
        for identifier in ids:
            if identifier == allowed_id:
                continue
            values = self.json_docker("container", "inspect", identifier)
            require(isinstance(values, list) and len(values) == 1, "ownership-unconfirmed")
            for mount in values[0].get("Mounts", []):
                if mount.get("Type") == "bind" and mount.get("RW") is True:
                    source = Path(mount["Source"])
                    require(
                        not (
                            source == self.data
                            or source in self.data.parents
                            or self.data in source.parents
                        ),
                        "ownership-unconfirmed",
                    )

    def lease_identity(self):
        path = self.data / "vault.lock"
        if not os.path.lexists(path):
            raise Refused("lease-missing")
        content = private_read(path, self.state["uid"], self.state["gid"], 0)
        require(content == b"", "ownership-unconfirmed")
        info = path.lstat()
        return {"device": info.st_dev, "inode": info.st_ino}

    def acquire_lease(self, expected):
        require(self.lease_identity() == expected, "ownership-unconfirmed")
        fd = os.open(self.data / "vault.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            require(
                {"device": info.st_dev, "inode": info.st_ino} == expected, "ownership-unconfirmed"
            )
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Refused("lease-held") from None
            require(self.lease_identity() == expected, "ownership-unconfirmed")
            self.lease = fd
        except BaseException:
            os.close(fd)
            raise

    def cleanup_inspector(self, name, nonce, image_id, path):
        identifiers = (
            self.docker(
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                "name=^/" + name + "$",
                "--format",
                "{{.ID}}",
            )[1]
            .decode()
            .splitlines()
        )
        require(
            len(identifiers) <= 1 and all(HEX.fullmatch(item) for item in identifiers),
            "cleanup-unconfirmed",
        )
        if not identifiers:
            return
        values = self.json_docker("container", "inspect", identifiers[0])
        require(isinstance(values, list) and len(values) == 1, "cleanup-unconfirmed")
        value = values[0]
        config, host = value["Config"], value["HostConfig"]
        mounts = value["Mounts"]
        require(
            value["Id"] == identifiers[0]
            and value["Name"] == "/" + name
            and value["Image"] == image_id
            and config["Image"] == image_id
            and config.get("Labels", {}).get("io.streamskope.maintenance") == nonce,
            "cleanup-unconfirmed",
        )
        require(
            config.get("User") == str(self.state["uid"]) + ":" + str(self.state["gid"])
            and config.get("Entrypoint") == ["node"]
            and config.get("Cmd") == [self.policy["dataCompatibility"]["inspector"], "/data"],
            "cleanup-unconfirmed",
        )
        require(
            host.get("ReadonlyRootfs") is True
            and host.get("NetworkMode") == "none"
            and host.get("Privileged") is False
            and not host.get("CapAdd")
            and host.get("CapDrop") == ["ALL"]
            and host.get("SecurityOpt") in (["no-new-privileges"], ["no-new-privileges:true"]),
            "cleanup-unconfirmed",
        )
        require(
            len(mounts) == 1
            and mounts[0].get("Type") == "bind"
            and mounts[0].get("Source") == str(path)
            and mounts[0].get("Destination") == "/data"
            and mounts[0].get("RW") is False,
            "cleanup-unconfirmed",
        )
        self.docker("rm", "--force", identifiers[0])
        remaining = self.docker(
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            "name=^/" + name + "$",
            "--format",
            "{{.ID}}",
        )[1].strip()
        require(remaining == b"", "cleanup-unconfirmed")

    def preflight(self, inspector, data=None, *, target=None):
        require(inspector["inspector"], "unsupported-target")
        path = self.data if data is None else Path(data)
        if target is not None and not target["inspector"]:
            # A failed first mutation may leave v1 plus a new predecessor sidecar.
            # Its actual format remains v1, but an old image never qualified this layout.
            require(
                not any(
                    os.path.lexists(path / "queries" / ("kafka-queries.json." + family + suffix))
                    for family in ("pre-views-v1", "pre-records-v2")
                    for suffix in [""] + ["." + str(number) for number in range(1, 100)]
                ),
                "preflight-blocked",
            )
        nonce = str(uuid.uuid4())
        name = "streamskope-preflight-" + nonce
        try:
            code, output = self.docker(
                "run",
                "--rm",
                "--name",
                name,
                "--label",
                "io.streamskope.maintenance=" + nonce,
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--user",
                str(self.state["uid"]) + ":" + str(self.state["gid"]),
                "--mount",
                "type=bind,source=" + str(path) + ",target=/data,readonly",
                "--entrypoint",
                "node",
                inspector["imageId"],
                self.policy["dataCompatibility"]["inspector"],
                "/data",
                allow_failure=True,
            )
            require(code == 0, "preflight-blocked")
            return inspection(decode(output), self.policy, inspector["version"], target)
        finally:
            self.cleanup_inspector(name, nonce, inspector["imageId"], path)

    def ready(self):
        for _ in range(60):
            okay = True
            for route, expected in (
                ("/health", "status"),
                ("/__streamskope_session/status", "state"),
            ):
                code, output = self.run(
                    [
                        "curl",
                        "--noproxy",
                        "*",
                        "--fail",
                        "--silent",
                        "--connect-timeout",
                        "2",
                        "--max-time",
                        "3",
                        self.origin + route,
                    ],
                    env=self.environment,
                    allow_failure=True,
                    timeout=5,
                )
                try:
                    value = decode(output)
                    okay = (
                        okay
                        and code == 0
                        and value.get(expected) == "locked"
                        and (route == "/health" or value.get("setupRequired") is False)
                    )
                except (ValueError, TypeError, AttributeError, Refused):
                    okay = False
            if okay:
                return
            time.sleep(1)
        raise Refused("candidate-unavailable")

    def capacity(self, entries):
        available = os.statvfs(self.root)
        needed = sum(item["bytes"] for item in entries) * 2 + 64 * 1024 * 1024
        require(available.f_bavail * available.f_frsize >= needed, "backup-unavailable")

    def save_journal(self, phase=None):
        if phase is not None:
            self.journal["phase"] = phase
        atomic_json(self.journal_path, self.journal, self.uid, self.gid)

    def load_journal(self):
        value = decode(private_read(self.journal_path, self.uid, self.gid))
        exact(
            value,
            {
                "schemaVersion",
                "transactionId",
                "operation",
                "phase",
                "originalRecord",
                "originalSha256",
                "nextRecord",
                "nextSha256",
                "from",
                "to",
                "originalContainer",
                "lease",
                "backupAttempt",
                "backup",
                "candidateId",
            },
        )
        require(
            type(value["schemaVersion"]) is int
            and value["schemaVersion"] == 1
            and uuid_text(value["transactionId"])
            and value["operation"] in ("upgrade", "rollback")
            and value["phase"] in PHASES
        )
        original = installation(value["originalRecord"])
        upcoming = installation(value["nextRecord"])
        require(
            value["nextRecord"]["schemaVersion"] == 2
            and all(original[key] == upcoming[key] for key in OWNER_KEYS)
            and upcoming["previous"] == original["current"]
        )
        require(
            isinstance(value["originalSha256"], str)
            and HEX.fullmatch(value["originalSha256"])
            and value["nextSha256"] == digest(canonical(value["nextRecord"]))
        )
        for field, expected in (("from", original["current"]), ("to", upcoming["current"])):
            exact(
                value[field],
                POINTER_KEYS
                | {"reference", "imageId", "deliveryScope", "contract", "inspector", "topology"},
            )
            release = value[field]
            require({key: release[key] for key in POINTER_KEYS} == expected)
            require(
                type(release["inspector"]) is bool
                and release["contract"] == self.policy["dataCompatibility"]["contract"]
                and release["topology"] == "streamskope-" + release["version"] + ".clab.yml"
            )
            require(
                isinstance(release["imageId"], str)
                and re.fullmatch(r"sha256:[a-f0-9]{64}", release["imageId"])
            )
            require(release["deliveryScope"] in ("public-registry", "local-staged"))
            if release["deliveryScope"] == "public-registry":
                require(
                    re.fullmatch(
                        r"ghcr\.io/asadarafat/streamskope:"
                        + re.escape(release["version"])
                        + r"@sha256:[a-f0-9]{64}",
                        release["reference"],
                    )
                )
            else:
                require(release["reference"] == "streamskope:" + release["version"])
        owner = exact(value["originalContainer"], {"id", "startedAt", "restartCount"})
        require(
            isinstance(owner["id"], str)
            and HEX.fullmatch(owner["id"])
            and isinstance(owner["startedAt"], str)
            and len(owner["startedAt"]) <= 64
            and integer(owner["restartCount"])
        )
        lease = exact(value["lease"], {"device", "inode"})
        require(
            integer(lease["device"])
            and integer(lease["inode"], 1)
            and integer(value["backupAttempt"], 0, 16)
        )
        require(
            value["candidateId"] is None
            or (isinstance(value["candidateId"], str) and HEX.fullmatch(value["candidateId"]))
        )
        if value["phase"] in ("deployed", "committed"):
            require(value["candidateId"] is not None)
        if value["backup"] is not None:
            self.parse_backup(value["backup"], value["transactionId"], value["backupAttempt"])
        if value["phase"] in ("backed-up", "retiring", "retired", "deployed", "committed"):
            require(value["backup"] is not None)
        self.journal, self.transaction_id = value, value["transactionId"]
        return value

    @staticmethod
    def parse_backup(value, transaction_id, attempt):
        exact(value, {"path", "inventorySha256", "dataSnapshotSha256"})
        require(
            value["path"] == "backups/" + transaction_id + "/attempt-" + str(attempt)
            and integer(attempt, 1, 16)
        )
        require(
            all(
                isinstance(value[key], str) and HEX.fullmatch(value[key])
                for key in ("inventorySha256", "dataSnapshotSha256")
            )
        )
        return value

    def backup(self, inspector, source):
        entries = inventory(self.data, self.state["uid"], self.state["gid"])
        self.capacity(entries)
        self.journal["backupAttempt"] += 1
        require(self.journal["backupAttempt"] <= 16, "backup-unavailable")
        self.save_journal()
        for path in (self.root / "backups", self.root / "backups" / self.transaction_id):
            if not os.path.lexists(path):
                os.mkdir(path, 0o700)
                os.chown(path, self.uid, self.gid)
                fsync_directory(path.parent)
            safe_directory(path, self.uid, self.gid)
        relative = (
            "backups/" + self.transaction_id + "/attempt-" + str(self.journal["backupAttempt"])
        )
        destination = self.root / relative
        require(not os.path.lexists(destination), "backup-unavailable")
        os.mkdir(destination, 0o700)
        os.chown(destination, self.uid, self.gid)
        fsync_directory(destination.parent)
        for entry in entries:
            copied = destination / "data" / entry["path"]
            if entry["type"] == "directory":
                os.mkdir(copied, entry["mode"])
                os.chown(copied, entry["uid"], entry["gid"])
            else:
                source_fd = os.open(
                    self.data / entry["path"], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                )
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
                fsync_directory(destination / "data" / entry["path"])
        require(
            inventory(destination / "data", self.state["uid"], self.state["gid"]) == entries
            and inventory(self.data, self.state["uid"], self.state["gid"]) == entries,
            "backup-unavailable",
        )
        verified = self.preflight(inspector, destination / "data")
        data_hash = digest(canonical(entries))
        document = {
            "schemaVersion": 1,
            "transactionId": self.transaction_id,
            "attempt": self.journal["backupAttempt"],
            "source": self.identity(source),
            "entries": entries,
            "dataSnapshotSha256": data_hash,
            "inspection": verified,
        }
        atomic_json(destination / "inventory.json", document, self.uid, self.gid)
        fsync_directory(destination)
        self.journal["backup"] = {
            "path": relative,
            "inventorySha256": digest(canonical(document)),
            "dataSnapshotSha256": data_hash,
        }
        self.save_journal("backed-up")

    def verify_backup(self, inspector):
        backup = self.parse_backup(
            self.journal["backup"], self.transaction_id, self.journal["backupAttempt"]
        )
        directory = self.root / backup["path"]
        safe_directory(directory, self.uid, self.gid)
        content = private_read(directory / "inventory.json", self.uid, self.gid, MAX_OUTPUT)
        require(digest(content) == backup["inventorySha256"], "backup-unavailable")
        value = decode(content)
        exact(
            value,
            {
                "schemaVersion",
                "transactionId",
                "attempt",
                "source",
                "entries",
                "dataSnapshotSha256",
                "inspection",
            },
        )
        require(
            value["schemaVersion"] == 1
            and value["transactionId"] == self.transaction_id
            and value["attempt"] == self.journal["backupAttempt"]
            and value["source"] == self.identity(self.journal["from"])
            and value["dataSnapshotSha256"] == backup["dataSnapshotSha256"]
            and digest(canonical(value["entries"])) == backup["dataSnapshotSha256"],
            "backup-unavailable",
        )
        require(
            inventory(directory / "data", self.state["uid"], self.state["gid"]) == value["entries"],
            "backup-unavailable",
        )
        inspection(value["inspection"], self.policy, inspector["version"], self.journal["to"])
        require(
            digest(canonical(inventory(self.data, self.state["uid"], self.state["gid"])))
            == backup["dataSnapshotSha256"],
            "recovery-required",
        )

    def stopped(self, container):
        state, original = container["State"], self.journal["originalContainer"]
        require(
            container["Id"] == original["id"]
            and state["StartedAt"] == original["startedAt"]
            and container["RestartCount"] == original["restartCount"],
            "cleanup-unconfirmed",
        )
        require(
            state["Running"] is False
            and state.get("Status") == "exited"
            and state.get("ExitCode") == 0
            and state.get("OOMKilled") is False
            and not state.get("Dead", False)
            and not state.get("Error", ""),
            "cleanup-unconfirmed",
        )

    def deploy(self, target):
        require(self.named_container() is None, "recovery-required")
        self.conflicts(None)
        env = {
            **self.environment,
            "STREAMSKOPE_IMAGE": target["reference"],
            "STREAMSKOPE_UID": str(self.state["uid"]),
            "STREAMSKOPE_GID": str(self.state["gid"]),
            "STREAMSKOPE_HOST_BIND": "127.0.0.1",
            "STREAMSKOPE_HOST_PORT": str(self.state["port"]),
            "STREAMSKOPE_PUBLIC_ORIGIN": self.origin,
        }
        clab = shutil.which("containerlab") or shutil.which("clab")
        require(clab is not None, "candidate-unavailable")
        self.run(
            [
                clab,
                "--runtime",
                "docker",
                "deploy",
                "--topo",
                str(self.root / target["topology"]),
                "--name",
                self.config["lab"],
                "--network",
                self.config["network"],
                "--max-workers",
                "1",
            ],
            env=env,
            timeout=180,
        )
        candidate = self.owned(target)
        self.journal["candidateId"] = candidate["Id"]
        self.save_journal("deployed")

    def archive(self):
        require(
            digest(private_read(self.state_path, self.uid, self.gid)) == self.journal["nextSha256"],
            "recovery-required",
        )
        self.save_journal("committed")
        directory = self.root / self.journal["backup"]["path"]
        safe_directory(directory, self.uid, self.gid)
        archived = directory / "transaction.json"
        if os.path.lexists(archived):
            require(
                private_read(archived, self.uid, self.gid) == canonical(self.journal),
                "recovery-required",
            )
            os.unlink(self.journal_path)
        else:
            os.rename(self.journal_path, archived)
        fsync_directory(directory)
        fsync_directory(self.root)

    def result(self, operation, outcome, current, previous=None):
        return {
            "schemaVersion": 1,
            "operation": operation,
            "outcome": outcome,
            "current": self.identity(current),
            "previous": None if previous is None else self.identity(previous),
            "transactionId": self.transaction_id,
            "backup": None if self.journal is None else self.journal["backup"],
            "url": self.origin + "/",
        }

    def resume(self, operation):
        journal = self.journal
        actual = private_read(self.state_path, self.uid, self.gid)
        if digest(actual) == journal["nextSha256"]:
            require(decode(actual) == journal["nextRecord"], "recovery-required")
            self.archive()
            return self.result(operation, "recovered", journal["to"], journal["from"])
        source = self.release({key: journal["from"][key] for key in POINTER_KEYS})
        target = self.release({key: journal["to"][key] for key in POINTER_KEYS})
        require(source == journal["from"] and target == journal["to"], "recovery-required")
        require(
            digest(actual) == journal["originalSha256"]
            and decode(actual) == journal["originalRecord"]
            and journal["phase"] != "committed",
            "recovery-required",
        )
        inspector = target if target["inspector"] else source
        require(
            inspector["inspector"] and source["contract"] == target["contract"],
            "unsupported-target",
        )
        original_id = journal["originalContainer"]["id"]
        if journal["phase"] == "intent":
            original = self.owned(source, original_id)
            require(
                original["State"]["StartedAt"] == journal["originalContainer"]["startedAt"]
                and original["RestartCount"] == journal["originalContainer"]["restartCount"],
                "cleanup-unconfirmed",
            )
            self.conflicts(original_id)
            if original["State"]["Running"]:
                require(self.lease_identity() == journal["lease"], "ownership-unconfirmed")
                self.preflight(inspector, target=target)
                self.capacity(inventory(self.data, self.state["uid"], self.state["gid"]))
                try:
                    self.docker("stop", "--time", "120", original_id, timeout=150)
                except Refused:
                    raise Refused("cleanup-unconfirmed") from None
            self.stopped(self.owned(source, original_id))
            self.acquire_lease(journal["lease"])
            self.save_journal("stopped")
        else:
            if journal["phase"] in ("stopped", "backed-up"):
                self.stopped(self.owned(source, original_id))
            elif journal["phase"] == "retiring" and self.named_container() is not None:
                self.stopped(self.owned(source, original_id))
            elif journal["phase"] == "retired":
                require(self.named_container() is None, "recovery-required")
            elif journal["phase"] == "deployed":
                self.owned(target, journal["candidateId"])
            self.acquire_lease(journal["lease"])
        self.conflicts(
            journal["candidateId"]
            if journal["phase"] == "deployed"
            else (None if journal["phase"] == "retired" else original_id)
        )
        self.preflight(inspector, target=target)
        if journal["phase"] == "stopped":
            self.backup(inspector, source)
        self.verify_backup(inspector)
        if journal["phase"] == "backed-up":
            self.stopped(self.owned(source, original_id))
            self.save_journal("retiring")
        if journal["phase"] == "retiring":
            if self.named_container() is not None:
                self.stopped(self.owned(source, original_id))
                self.docker("rm", original_id)
            require(self.named_container() is None, "recovery-required")
            self.save_journal("retired")
        if journal["phase"] == "retired":
            self.deploy(target)
        before_ready = self.owned(target, journal["candidateId"])
        require(
            before_ready["State"]["Running"] is True
            and before_ready["State"]["Status"] == "running",
            "candidate-unavailable",
        )
        self.ready()
        after_ready = self.owned(target, journal["candidateId"])
        require(
            after_ready["State"]["Running"] is True
            and after_ready["State"]["Status"] == "running"
            and after_ready["State"]["StartedAt"] == before_ready["State"]["StartedAt"]
            and after_ready["RestartCount"] == before_ready["RestartCount"],
            "candidate-unavailable",
        )
        self.conflicts(journal["candidateId"])
        self.verify_backup(inspector)
        require(
            self.lease_identity() == journal["lease"]
            and digest(private_read(self.state_path, self.uid, self.gid))
            == journal["originalSha256"],
            "recovery-required",
        )
        final_owner = self.owned(target, journal["candidateId"])
        require(
            final_owner["State"]["Running"] is True
            and final_owner["State"]["Status"] == "running"
            and final_owner["State"]["StartedAt"] == before_ready["State"]["StartedAt"]
            and final_owner["RestartCount"] == before_ready["RestartCount"],
            "candidate-unavailable",
        )
        atomic_json(self.state_path, journal["nextRecord"], self.uid, self.gid)
        self.archive()
        return self.result(
            operation, "recovered" if operation == "recover" else "committed", target, source
        )

    def execute(self, operation):
        require(operation in ("check", "upgrade", "rollback", "recover"))
        self.operator()
        if os.path.lexists(self.journal_path):
            require(operation == "recover", "recovery-required")
            self.load_journal()
            return self.resume(operation)
        source = self.release(self.state["current"])
        if operation == "recover":
            self.owned(source)
            previous = (
                None if self.state["previous"] is None else self.release(self.state["previous"])
            )
            return self.result(operation, "unchanged", source, previous)
        selected = self.state["previous"] if operation == "rollback" else self.config["target"]
        require(selected is not None, "unsupported-target")
        target = self.release(selected)
        if selected == self.state["current"]:
            self.owned(source)
            previous = (
                None if self.state["previous"] is None else self.release(self.state["previous"])
            )
            return self.result(operation, "unchanged", source, previous)
        if operation != "rollback":
            require(
                newer(selected["version"], self.state["current"]["version"]), "unsupported-target"
            )
        owner = self.owned(source)
        lease = self.lease_identity()
        self.conflicts(owner["Id"])
        inspector = target if target["inspector"] else source
        require(source["contract"] == target["contract"], "unsupported-target")
        self.preflight(inspector, target=target)
        self.capacity(inventory(self.data, self.state["uid"], self.state["gid"]))
        if operation == "check":
            previous = (
                None if self.state["previous"] is None else self.release(self.state["previous"])
            )
            return self.result(operation, "checked", source, previous)
        self.transaction_id = str(uuid.uuid4())
        next_record = {**self.state, "current": selected, "previous": self.state["current"]}
        self.journal = {
            "schemaVersion": 1,
            "transactionId": self.transaction_id,
            "operation": operation,
            "phase": "intent",
            "originalRecord": self.original,
            "originalSha256": digest(self.original_bytes),
            "nextRecord": next_record,
            "nextSha256": digest(canonical(next_record)),
            "from": source,
            "to": target,
            "originalContainer": {
                "id": owner["Id"],
                "startedAt": owner["State"]["StartedAt"],
                "restartCount": owner["RestartCount"],
            },
            "lease": lease,
            "backupAttempt": 0,
            "backup": None,
            "candidateId": None,
        }
        self.save_journal()
        return self.resume(operation)

    def close(self):
        if self.lease is not None:
            os.close(self.lease)
            self.lease = None


def newer(candidate, current):
    """SemVer precedence, including numeric prerelease identifiers; build metadata is not used here."""

    def parts(value):
        require(isinstance(value, str) and VERSION.fullmatch(value), "unsupported-target")
        core, dash, preview = value.partition("-")
        identifiers = preview.split(".") if dash else None
        if identifiers is not None:
            require(
                all(
                    item and not (item.isdigit() and len(item) > 1 and item.startswith("0"))
                    for item in identifiers
                ),
                "unsupported-target",
            )
        return tuple(int(item) for item in core.split(".")), identifiers

    left, lp = parts(candidate)
    right, rp = parts(current)
    if left != right:
        return left > right
    if lp is None or rp is None:
        return lp is None and rp is not None
    for a, b in zip(lp, rp):
        if a == b:
            continue
        if a.isdigit() and b.isdigit():
            return int(a) > int(b)
        if a.isdigit() != b.isdigit():
            return not a.isdigit()
        return a > b
    return len(lp) > len(rp)


def read_current(root, uid, gid):
    safe_directory(root, uid, gid)
    require(not os.path.lexists(Path(root) / "maintenance.json"), "recovery-required")
    state = installation(decode(private_read(Path(root) / "installation.json", uid, gid)))
    return [
        state["current"][key]
        for key in ("version", "sourceRevision", "topologySha256", "manifestSha256")
    ] + [state[key] for key in ("uid", "gid", "home", "operatorUid", "port")]


def main(arguments):
    engine, operation = None, "unknown"
    try:
        os.umask(0o077)
        if arguments and arguments[0] == "read-current":
            require(len(arguments) == 4)
            for value in read_current(arguments[1], int(arguments[2]), int(arguments[3])):
                print(value)
            return 0
        require(len(arguments) == 14 and arguments[0] == "maintain")
        (
            _,
            operation,
            root,
            uid,
            gid,
            arch,
            lab,
            network,
            container,
            version,
            source,
            topology,
            manifest,
            stage,
        ) = arguments
        require(
            operation in ("check", "upgrade", "rollback", "recover") and arch in ("amd64", "arm64")
        )
        require(
            all(
                re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}", item)
                for item in (lab, network, container)
            )
        )
        policy = decode(base64.b64decode(POLICY_B64, validate=True))
        local = decode(base64.b64decode(LOCAL_B64, validate=True))
        config = {
            "root": root,
            "stateUid": int(uid),
            "stateGid": int(gid),
            "arch": arch,
            "lab": lab,
            "network": network,
            "container": container,
            "stage": stage,
            "target": pointer(
                {
                    "version": version,
                    "sourceRevision": source,
                    "topologySha256": topology,
                    "manifestSha256": manifest,
                }
            ),
        }
        engine = Maintenance(config, policy, local)
        result = engine.execute(operation)
        sys.stdout.buffer.write(canonical(result))
        return 0
    except (Exception, KeyboardInterrupt) as error:
        reason = (
            error.reason
            if isinstance(error, Refused)
            else (
                "recovery-required"
                if engine is not None and engine.transaction_id is not None
                else "invalid-state"
            )
        )
        if arguments and arguments[0] == "read-current":
            sys.stderr.write(
                "StreamSkope: " + reason + "; preserve deployment data and use explicit recovery.\n"
            )
        else:
            sys.stdout.buffer.write(
                canonical(
                    {
                        "schemaVersion": 1,
                        "operation": (
                            operation
                            if operation in ("check", "upgrade", "rollback", "recover")
                            else "unknown"
                        ),
                        "outcome": "blocked",
                        "reason": reason,
                        "transactionId": None if engine is None else engine.transaction_id,
                    }
                )
            )
        return 2
    finally:
        if engine is not None:
            engine.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
