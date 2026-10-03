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


def published_release(root=ROOT, environment=None):
    """Require a published desktop event, its exact checkout and stamped version."""
    environment = os.environ if environment is None else environment
    if environment.get("GITHUB_EVENT_NAME") != "release":
        raise ValueError("Pages publication requires a published desktop release event")
    event_path = environment.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise ValueError("Pages publication requires the release event payload")
    event = json.loads(Path(event_path).read_text())
    release = event.get("release") if isinstance(event, dict) else None
    if not isinstance(release, dict):
        raise ValueError("Pages publication requires release metadata")
    tag = release.get("tag_name", "")
    version = release_version(tag) if isinstance(tag, str) else None
    if (event.get("action") != "published" or release.get("draft") is not False
            or not version or tag != environment.get("GITHUB_REF_NAME")
            or environment.get("GITHUB_REF") != f"refs/tags/{tag}"
            or not isinstance(release.get("body"), str) or not release["body"].strip()):
        raise ValueError("Pages requires the exact published desktop release and its notes")
    if json.loads((Path(root) / "package.json").read_text())["version"] != version:
        raise ValueError("Published desktop tag differs from the stamped application version")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if not re.fullmatch(r"[a-f0-9]{40}", revision) or revision != environment.get("GITHUB_SHA"):
        raise ValueError("Pages checkout differs from the published release source")
    return release


def prepare_publication(root=ROOT, environment=None):
    """Materialize the publication event's notes and downloads in its build checkout."""
    root = Path(root)
    release = published_release(root, environment)
    tag = release["tag_name"]
    version = release_version(tag)
    config = root / "website/zensical.toml"
    source, count = re.subn(r'^desktop_release = "[^"]+"$',
                            f'desktop_release = "{tag}"', config.read_text(), flags=re.M)
    if count != 1:
        raise ValueError("Pages requires one documented desktop release setting")
    index = root / "website/docs/releases/index.md"
    status = "Published prerelease" if release.get("prerelease") else "Published release"
    row = f"| [{tag}]({tag}.md) | {version} | {status}; notes from the publication event |"
    contents, rows = re.subn(r'^\|[^\n]*\(unreleased\.md\)[^\n]*$', row,
                             index.read_text(), flags=re.M)
    if rows != 1 and not (rows == 0 and row in contents):
        raise ValueError("Release index needs one unreleased row or this publication's row")
    notes = (f"---\ntitle: StreamSkope {tag}\nrelease_version: {version}\n"
             f"release_tag: {tag}\n---\n\n" + release["body"])
    qualification = root / "website/docs/guide/qualification.md"
    evidence = publication_qualification(qualification.read_text(), release,
                                         subprocess.check_output(
                                             ["git", "rev-parse", "HEAD"], cwd=root, text=True).strip())
    # Validate all inputs before changing the disposable checkout. Main is never stamped.
    (root / f"website/docs/releases/{tag}.md").write_text(notes)
    config.write_text(source)
    index.write_text(contents)
    qualification.write_text(evidence)
    (root / "website/docs/releases/unreleased.md").unlink(missing_ok=True)


def publication_qualification(content, release, revision):
    """Link recorded evidence without inferring test outcomes from publication."""
    tag = release["tag_name"]
    repository = "https://github.com/asadarafat/streamskope"
    name = f"qualification-{tag}.json"
    url = f"{repository}/releases/download/{tag}/{name}"
    assets = release.get("assets", [])
    recorded = isinstance(assets, list) and any(
        isinstance(asset, dict) and asset.get("name") == name
        and asset.get("browser_download_url") == url and asset.get("state") == "uploaded"
        and type(asset.get("size")) is int and asset["size"] > 0
        for asset in assets)
    content = re.sub(r"<!-- publication-qualification -->[\s\S]*?"
                     r"<!-- /publication-qualification -->\n*", "", content)
    content = re.sub(r"^## Published release:", "## Historical qualification:", content, flags=re.M)
    if not re.search(r"^## Historical qualification:", content, re.M):
        raise ValueError("Qualification page needs a release evidence section")
    report = (f"The [source-specific qualification report]({url}) was included in the "
              "publication event. Read its executed checks, source identity, environment "
              "and limitations; the link alone does not establish that every check passed."
              if recorded else
              "No source-specific qualification report was included in the publication event. "
              "Live tests and other local rehearsals are unrecorded here; publication does "
              "not mark them as passed.")
    block = (f"<!-- publication-qualification -->\n## Published release: {tag}\n\n"
             f"These pages describe [{tag}](../releases/{tag}.md) at source "
             f"[`{revision[:7]}`]({repository}/commit/{revision}). The release notes link "
             "the packaging workflow; earlier release results below are historical.\n\n"
             f"{report}\n<!-- /publication-qualification -->\n\n")
    return re.sub(r"(?=^## Historical qualification:)", lambda _: block,
                  content, count=1, flags=re.M)


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
    # Public guides must describe this release's source, never a newer main checkout.
    published = environment.get("STREAMSKOPE_DOCS_PUBLISH") == "1"
    if published:
        event_release = published_release(root, environment)
        if release != event_release["tag_name"] or source_release != release:
            raise ValueError("Published documentation and downloads must match the desktop release")
    if desktop_tag and tag != source_release:
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
    routes = ("", "start/installation/", "plugins/eda/", "plugins/nsp/", "guide/qualification/")
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
                if route == "guide/qualification/" and f"Published release: {release}" not in html:
                    raise ValueError("Published qualification page describes another release")
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
