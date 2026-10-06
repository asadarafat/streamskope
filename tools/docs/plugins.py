"""Source requirements and separately verified plugin publication availability."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit

from .publication import publication_identity

SNAPSHOT = ".cache/docs/plugin-publications.json"
RELEASE_ROOT = "https://github.com/asadarafat/streamskope/releases"
PORTABLE_MARKER = "<!-- portable-plugin-downloads -->"


def official_url(value, path):
    if not isinstance(value, str):
        raise ValueError("Plugin publication needs an official release URL")
    url = urlsplit(value)
    if (url.scheme != "https" or url.netloc != "github.com" or url.query or url.fragment
            or unquote(value) != f"{RELEASE_ROOT}/{path}"):
        raise ValueError("Plugin publication URL differs from its official release")


def validate_portable(package):
    system = package["id"].split(".")[1]
    tag = package.get("release_tag")
    if not isinstance(tag, str) or not re.fullmatch(
            rf"(?:v|plugins/{system}/v)[0-9][0-9A-Za-z.+_-]{{0,191}}", tag):
        raise ValueError("Invalid verified plugin release identity")
    official_url(package.get("release_url"), f"tag/{tag}")
    compatibility = package.get("compatibility")
    if compatibility is not None:
        if not isinstance(compatibility, dict):
            raise ValueError("Invalid verified plugin compatibility")
        host, target = compatibility.get("streamskope"), compatibility.get("target")
        if (not isinstance(host, dict) or not isinstance(target, dict)
                or target.get("system") != system):
            raise ValueError("Invalid verified plugin compatibility")
        for version in (host.get("minimum"), target.get("minimum"), target.get("maximum")):
            if not isinstance(version, str) or not re.fullmatch(r"v?[0-9][0-9A-Za-z.+_-]{0,191}", version):
                raise ValueError("Invalid verified plugin compatibility version")
        maximum = host.get("maximumExclusive")
        if ((maximum is not None and (not isinstance(maximum, str)
                or not re.fullmatch(r"[0-9][0-9A-Za-z.+_-]{0,191}", maximum)))
                or (package["api"] >= 4 and maximum is None)):
            raise ValueError("Invalid verified plugin compatibility version")
    elif package["api"] >= 3:
        raise ValueError("Verified plugin requires its published compatibility")
    if "portable" not in package:
        raise ValueError("Verified plugin publication must record portable availability")
    portable = package["portable"]
    if portable is None:
        return
    version = package["version"] if package["version"].startswith("v") else f"v{package['version']}"
    name = f"streamskope-{system}-portable-{version}.skope-plugin"
    if (not isinstance(portable, dict) or portable.get("name") != name
            or not isinstance(portable.get("sha256"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", portable["sha256"])
            or type(portable.get("size")) is not int or not 0 < portable["size"] <= 32 * 1024 * 1024
            or not isinstance(portable.get("publisher"), str) or not portable["publisher"]
            or len(portable["publisher"]) > 200 or re.search(r"[\x00-\x1f\x7f]", portable["publisher"])
            or not isinstance(portable.get("publisher_key_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", portable["publisher_key_id"])):
        raise ValueError("Invalid verified portable plugin metadata")
    official_url(portable.get("url"), f"download/{tag}/{name}")


def validate_snapshot(snapshot, identity):
    if (not isinstance(snapshot, dict)
            or any(snapshot.get(key) != value for key, value in identity.items())
            or not isinstance(snapshot.get("checked_at"), str)
            or not isinstance(snapshot.get("packages"), list)):
        raise ValueError("Plugin availability snapshot differs from the documentation identity")
    try:
        checked = datetime.fromisoformat(snapshot["checked_at"])
        if checked.tzinfo is None:
            raise ValueError()
    except ValueError as error:
        raise ValueError("Plugin availability snapshot needs a timestamp with timezone") from error
    packages = {}
    for package in snapshot["packages"]:
        if (not isinstance(package, dict) or package.get("id") not in {"streamskope.eda", "streamskope.nsp"}
                or not isinstance(package.get("version"), str)
                or not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.+_-]{0,191}", package["version"])
                or type(package.get("api")) is not int or package["api"] <= 0
                or not isinstance(package.get("sha256"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", package["sha256"])):
            raise ValueError("Invalid verified plugin publication metadata")
        if package["id"] in packages:
            raise ValueError("Duplicate verified plugin publication identity")
        validate_portable(package)
        packages[package["id"]] = package
    return packages


def capture_publications(root):
    """Reuse installation's compatibility and digest checks; do not duplicate its catalog."""
    root = Path(root)
    identity = publication_identity(root)
    packages = json.loads(subprocess.check_output(
        ["node", "--import", "tsx", "tools/docs/plugin-publications.ts", identity["desktop_release"]],
        cwd=root, text=True, timeout=90, env=os.environ))
    snapshot = {**identity, "checked_at": datetime.now(timezone.utc).isoformat(), "packages": packages}
    validate_snapshot(snapshot, identity)
    path = root / SNAPSHOT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2) + "\n")
    print(f"Plugin availability captured for {identity['desktop_release']}: {len(packages)} compatible packages")
    return snapshot


def plugin_context(root, published=False):
    root = Path(root)
    offline = root / "website/docs/plugins/offline.md"
    if offline.is_file() and (offline.read_text().count(PORTABLE_MARKER) != 1
            or not re.search(r"^plugin_portable_downloads: true$", offline.read_text(), re.M)):
        raise ValueError("Offline plugin guide needs exactly one portable download marker and metadata")
    packages = {}
    checked_at = ""
    if published:
        path = root / SNAPSHOT
        if not path.is_file():
            raise ValueError("Published plugin notices require a verified availability snapshot; run docs prepare")
        snapshot = json.loads(path.read_text())
        packages = validate_snapshot(snapshot, publication_identity(root))
        checked_at = datetime.fromisoformat(snapshot["checked_at"]).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rows = []
    for file in sorted((root / "plugins").glob("*/manifest.json")):
        manifest = json.loads(file.read_text())
        target = manifest["compatibility"]["target"]
        fields = {"name": manifest["name"], "version": manifest["version"],
                  "api": manifest["apiVersion"], "minimum_host": manifest["compatibility"]["streamskope"]["minimum"],
                  "maximum_host_exclusive": manifest["compatibility"]["streamskope"]["maximumExclusive"],
                  "system": target["system"], "minimum": target["minimum"], "maximum": target["maximum"],
                  "availability": "unavailable" if published else "unchecked", "checked_at": checked_at,
                  "portable_availability": "unavailable" if published else "unchecked"}
        package = packages.get(manifest["id"])
        if package:
            fields.update(availability="published", published_version=package["version"],
                          published_api=package["api"], published_sha256=package["sha256"],
                          published_release_url=package["release_url"])
            compatibility = package["compatibility"]
            if compatibility:
                host, target = compatibility["streamskope"], compatibility["target"]
                fields.update(published_minimum_host=host["minimum"],
                              published_maximum_host_exclusive=host.get("maximumExclusive", ""),
                              published_system=target["system"], published_minimum_target=target["minimum"],
                              published_maximum_target=target["maximum"])
            if package["portable"]:
                fields.update(portable_availability="published", **{
                    f"portable_{key}": value for key, value in package["portable"].items()})
        rows.append(fields)
    return rows
