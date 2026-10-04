"""Source requirements and separately verified plugin publication availability."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess

from .publication import publication_identity

SNAPSHOT = ".cache/docs/plugin-publications.json"


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
        packages[package["id"]] = package
    return packages


def capture_publications(root):
    """Reuse installation's compatibility and digest checks; do not duplicate its catalog."""
    root = Path(root)
    identity = publication_identity(root)
    script = r'''
const {OfficialPluginCatalog} = require('./src/platform/node/plugins/catalog.ts');
const fetcher = (input, options) => {
  const headers = new Headers(options.headers);
  if (new URL(input).hostname === 'api.github.com' && process.env.GH_TOKEN)
    headers.set('Authorization', `Bearer ${process.env.GH_TOKEN}`);
  return fetch(input, {...options, headers});
};
new OfficialPluginCatalog(fetcher, process.env.STREAMSKOPE_DOCS_DESKTOP).list()
  .then(entries => process.stdout.write(JSON.stringify(entries.map(({manifest, sha256}) => ({
    id: manifest.id, version: manifest.version, api: manifest.apiVersion, sha256
  })))))
  .catch(error => { process.stderr.write(`Plugin availability failed: ${error.message}\n`); process.exitCode = 1; });
'''
    packages = json.loads(subprocess.check_output(
        ["node", "--import", "tsx", "--eval", script], cwd=root, text=True, timeout=60,
        env={**os.environ, "STREAMSKOPE_DOCS_DESKTOP": identity["desktop_release"]}))
    snapshot = {**identity, "checked_at": datetime.now(timezone.utc).isoformat(), "packages": packages}
    validate_snapshot(snapshot, identity)
    path = root / SNAPSHOT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2) + "\n")
    print(f"Plugin availability captured for {identity['desktop_release']}: {len(packages)} compatible packages")
    return snapshot


def plugin_context(root, published=False):
    root = Path(root)
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
                  "availability": "unavailable" if published else "unchecked", "checked_at": checked_at}
        package = packages.get(manifest["id"])
        if package:
            fields.update(availability="published", published_version=package["version"],
                          published_api=package["api"], published_sha256=package["sha256"])
        rows.append(fields)
    return rows
