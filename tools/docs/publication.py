"""Release identity and bounded public-site verification."""
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]

# New release identities use SemVer precedence. Historical +build.N tags remain
# valid download destinations, but cannot identify a new release from source.
SEMVER = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"


def release_version(tag, historical=False):
    """Return the installer version for an exact supported desktop tag."""
    suffix = r"(?:\+build\.[1-9][0-9]*)?" if historical else ""
    match = re.fullmatch(r"v(" + SEMVER + r")" + suffix, tag)
    return match.group(1) if match else None


def validate_notes(root, tag, version):
    notes = root / "website/docs/releases" / f"{tag}.md"
    content = notes.read_text() if notes.is_file() else ""
    metadata = re.match(r"---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)", content)
    fields = metadata.group(1) if metadata else ""
    if not all(re.findall(r"^" + name + r":\s*([^\r\n]*)$", fields, re.M) == [value]
               for name, value in (("release_tag", tag), ("release_version", version))):
        raise ValueError("Release needs notes for its exact tag and application version")


def validate_unreleased_notes(root):
    """Development notes must not assign an application release identity."""
    notes = root / "website/docs/releases/unreleased.md"
    content = notes.read_text() if notes.is_file() else ""
    metadata = re.match(r"---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)", content)
    fields = metadata.group(1) if metadata else ""
    if (re.findall(r"^unreleased:\s*([^\r\n]*)$", fields, re.M) != ["true"]
            or re.search(r"^release_(?:tag|version):", fields, re.M)
            or not re.search(r"^# Unreleased changes\s*$", content, re.M)):
        raise ValueError("Development source needs unversioned unreleased notes")


def documentation_context(root=ROOT, environment=None):
    """Keep published downloads separate from development or CI-stamped source."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    project = tomllib.loads((root / "website/zensical.toml").read_text())["project"]
    release = project["extra"]["desktop_release"]
    version = release_version(release, historical=True)
    if not version:
        raise ValueError("Documented desktop release must be an exact release identity")
    validate_notes(root, release, version)
    source_version = json.loads((root / "package.json").read_text())["version"]
    if source_version == "0.0.0-dev":
        validate_unreleased_notes(root)
        source_release = "development"
    else:
        source_release = "v" + source_version
        if not release_version(source_release):
            raise ValueError("Source application version must be SemVer without build metadata")
        validate_notes(root, source_release, source_version)
    tag = environment.get("GITHUB_REF_NAME", "")
    desktop_tag = tag.startswith("v") and (
        environment.get("GITHUB_EVENT_NAME") == "release"
        or environment.get("GITHUB_REF_TYPE") == "tag"
        or environment.get("GITHUB_REF", "").startswith("refs/tags/")
    )
    # A stamped build must retain its exact identity. Pages always uses main,
    # even when publication of a historical release triggers the deployment.
    published = environment.get("STREAMSKOPE_DOCS_PUBLISH") == "1"
    if desktop_tag and tag != source_release and not published:
        raise ValueError("Desktop release tag differs from the source application version")
    return {
        "status": "Published documentation" if published else "Development documentation",
        "source_release": source_release,
    }


def publication_identity(root=ROOT):
    project = tomllib.loads((Path(root) / "website/zensical.toml").read_text())["project"]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    return {"revision": revision, "desktop_release": project["extra"]["desktop_release"]}


def verify_publication(url, revision, release, attempts=12, delay=5):
    """Bound CDN propagation waits; verify public bytes, not just deployment status."""
    if not revision or not release:
        raise ValueError("Publication verification requires --revision and --release")
    routes = ("", "start/installation/", "plugins/eda/", "plugins/nsp/")
    failure = None
    deadline = time.monotonic() + 90
    for attempt in range(attempts):
        try:
            def fetch(path):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError("Publication verification exceeded 90 seconds")
                with urlopen(url + path + "?revision=" + revision, timeout=min(5, remaining)) as response:
                    return response.read(2_000_000).decode("utf8")
            marker = json.loads(fetch("documentation.json"))
            if marker != {"revision": revision, "desktop_release": release}:
                raise ValueError("Published documentation marker is stale or mismatched")
            for route in routes:
                html = fetch(route)
                if f'name="streamskope-docs-revision" content="{revision}"' not in html:
                    raise ValueError(f"Published route is stale: {route}")
            for plugin in ("eda", "nsp"):
                html = fetch(f"guide/{plugin}/")
                if f"../../plugins/{plugin}/" not in html or "location.hash" not in html:
                    raise ValueError(f"Published {plugin} bookmark redirect is missing")
            print(f"Published docs verified: {revision}, {release}, {len(routes)} routes and 2 redirects")
            return
        except (OSError, ValueError) as error:
            failure = error
            if time.monotonic() >= deadline:
                break
            if attempt + 1 < attempts:
                time.sleep(delay)
    raise ValueError(f"Public documentation verification failed: {failure}")
