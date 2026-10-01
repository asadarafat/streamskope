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

def documentation_context(root=ROOT, environment=None):
    """Validate the documented build and reject mismatched desktop release tags."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    project = tomllib.loads((root / "website/zensical.toml").read_text())["project"]
    release = project["extra"]["desktop_release"]
    if not re.fullmatch(r"v\d+\.\d+\.\d+(?:\+build\.[1-9]\d*)?", release):
        raise ValueError("Documented desktop release must be an exact release identity")
    notes = root / "website/docs/releases" / f"{release}.md"
    if not notes.is_file() or not re.search(
        r"^release_tag: " + re.escape(release) + r"$", notes.read_text(), re.M,
    ):
        raise ValueError("Documented release needs notes for its exact tag")
    tag = environment.get("GITHUB_REF_NAME", "")
    desktop_tag = tag.startswith("v") and (
        environment.get("GITHUB_EVENT_NAME") == "release"
        or environment.get("GITHUB_REF_TYPE") == "tag"
        or environment.get("GITHUB_REF", "").startswith("refs/tags/")
    )
    # Shared tag CI must reject this before native packaging, not after publication.
    published = environment.get("STREAMSKOPE_DOCS_PUBLISH") == "1"
    if desktop_tag and tag != release and not published:
        raise ValueError("Desktop release tag differs from the documented desktop release")
    return {
        "status": "Published documentation" if published else "Development documentation",
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
