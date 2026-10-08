"""Derive desktop download links and verify published release assets without downloading them."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from urllib.parse import quote

if not __package__:
    # Pages invokes this file directly; module imports use tools/ as their root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from docs.publication import release_version, published_release


ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "asadarafat/streamskope"
DOWNLOAD_MARKER = "<!-- desktop-downloads -->"
PLATFORMS = (
    ("macOS, Apple Silicon", "darwin-arm64.dmg"),
    ("Windows x64", "win32-x64-Setup.exe"),
    ("Linux x64", "linux-x64.AppImage"),
)


def desktop_downloads(root=ROOT):
    """Use the documented release, independent of the development app version."""
    root = Path(root)
    project = tomllib.loads((root / "website/zensical.toml").read_text())["project"]
    tag = project["extra"]["desktop_release"]
    version = release_version(tag, historical=True)
    if not version:
        raise ValueError("Desktop downloads require an exact documented release identity")
    installation = (root / "website/docs/start/installation.md").read_text()
    if installation.count(DOWNLOAD_MARKER) != 1:
        raise ValueError("Installation page requires exactly one desktop download marker")
    release_root = f"https://github.com/{REPOSITORY}/releases"
    encoded_tag = quote(tag, safe="")
    download_root = f"{release_root}/download/{encoded_tag}"
    assets = []
    for platform, suffix in PLATFORMS:
        name = f"StreamSkope-{version}-{suffix}"
        assets.append({"platform": platform, "name": name, "url": f"{download_root}/{name}"})
    return {
        "tag": tag,
        "version": version,
        "release_url": f"{release_root}/tag/{encoded_tag}",
        "notes_path": f"releases/{tag}/",
        "checksum_url": f"{download_root}/SHA256SUMS",
        "assets": assets,
    }



CONTAINER_MARKER = "<!-- container-downloads -->"
BROWSER_INSTALLER_MARKER = "<!-- browser-installer -->"
BROWSER_INSTALLER_NAME = "install-browser-workbench.sh"


def browser_installation_capability(root):
    """The release's reviewed pages select its installer or historical manual layout."""
    docs = root / "website/docs"
    quickstart = docs / "start/containerlab.md"
    pages = {page: page.read_text() for page in docs.rglob("*.md")}
    source = pages[quickstart]

    def declares(page_source, name):
        header = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", page_source, re.S)
        if header is None:
            return False
        values = re.findall(rf"^{name}:\s*(.*?)\s*$", header[1], re.M)
        return values == ["true"]

    installer_pages = [page for page, text in pages.items()
                       if BROWSER_INSTALLER_MARKER in text or declares(text, "browser_installer")]
    containers = [(page, text.count(CONTAINER_MARKER)) for page, text in pages.items()
                  if CONTAINER_MARKER in text or declares(text, "container_downloads")]
    if installer_pages:
        if (installer_pages != [quickstart]
                or source.count(BROWSER_INSTALLER_MARKER) != 1
                or not declares(source, "browser_installer")):
            raise ValueError("Browser quickstart requires one installer marker and browser_installer: true")
        # Earlier installer releases kept manual deployment in the operations
        # guide. Newer docs separate that reference from everyday browser use.
        manual_pages = {docs / "guide/browser-deployment.md", docs / "guide/browser-host.md"}
        if (len(containers) != 1 or containers[0][0] not in manual_pages
                or containers[0][1] != 1
                or not declares(pages[containers[0][0]], "container_downloads")):
            raise ValueError("Browser deployment reference requires one manual download marker and container_downloads: true")
        return True
    if containers != [(quickstart, 1)]:
        raise ValueError("Historical Containerlab guide requires exactly one browser download marker")
    return False


def container_downloads(root=ROOT, environment=None):
    """Only exact publication-event assets establish browser availability."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    project = tomllib.loads((root / "website/zensical.toml").read_text())["project"]
    tag = project["extra"]["desktop_release"]
    version = release_version(tag, historical=True)
    if not version:
        raise ValueError("Browser downloads require the documented core release")
    installer_required = browser_installation_capability(root)
    base = f"https://github.com/{REPOSITORY}/releases"
    result = {"available": False, "installer_available": False,
              "tag": tag, "version": version,
              "release_url": f"{base}/tag/{quote(tag, safe='')}", "assets": []}
    # A main/source preview never infers publication from source version or filenames.
    if environment.get("STREAMSKOPE_DOCS_PUBLISH") != "1":
        return result
    release = published_release(root, environment)
    if release["tag_name"] != tag:
        raise ValueError("Browser availability differs from the documented release")
    download_root = f"{base}/download/{quote(tag, safe='')}"
    names = [
        ("Linux x64 Docker image", f"StreamSkope-{version}-container-linux-amd64.tar.gz"),
        ("Linux ARM64 Docker image", f"StreamSkope-{version}-container-linux-arm64.tar.gz"),
        ("Containerlab topology", f"streamskope-{version}.clab.yml"),
        ("Image identity and archive checksums", f"streamskope-{version}-container.json"),
    ]
    # Registry delivery starts with core 0.10.0. Older archive-only releases retain
    # their original four assets; newer releases must include the offline topology.
    numeric_version = tuple(int(part) for part in version.split("+", 1)[0].split("-", 1)[0].split("."))
    registry_delivery = installer_required or numeric_version >= (0, 10, 0)
    if registry_delivery:
        names.insert(3, ("Offline Containerlab topology", f"streamskope-{version}-offline.clab.yml"))
    if installer_required:
        names.insert(0, ("Browser workbench installer", BROWSER_INSTALLER_NAME))
    assets = release.get("assets", [])
    if not isinstance(assets, list):
        raise ValueError("Browser release assets must be a list")
    if not installer_required and not any(isinstance(asset, dict) and asset.get("name") in
               {name for _, name in names} for asset in assets):
        return result  # Historical releases can legitimately have desktop installers only.
    for label, name in names:
        matches = [asset for asset in assets if isinstance(asset, dict) and asset.get("name") == name]
        url = f"{download_root}/{name}"
        if len(matches) != 1 or matches[0].get("state") != "uploaded" or (
                type(matches[0].get("size")) is not int or matches[0]["size"] <= 0
                or matches[0].get("browser_download_url") != url):
            raise ValueError(f"Browser release requires one complete exact asset: {name}")
        result["assets"].append({"label": label, "name": name, "url": url})
    checksums = [asset for asset in assets if isinstance(asset, dict) and asset.get("name") == "SHA256SUMS"]
    checksum_url = f"{download_root}/SHA256SUMS"
    if len(checksums) != 1 or checksums[0].get("state") != "uploaded" or (
            type(checksums[0].get("size")) is not int or checksums[0]["size"] <= 0
            or checksums[0].get("browser_download_url") != checksum_url):
        raise ValueError("Browser release requires its exact published SHA256SUMS")
    result.update({"available": True, "checksum_url": checksum_url,
                   "registry_delivery": registry_delivery})
    if installer_required:
        installer_url = f"{download_root}/{BROWSER_INSTALLER_NAME}"
        latest_installer_url = f"{base}/latest/download/{BROWSER_INSTALLER_NAME}"
        result.update({"installer_available": True, "installer_url": installer_url,
                       "install_command": f"curl -fsSL {latest_installer_url} | sudo -E bash"})
    return result


def inspect_release_assets(downloads, release):
    """Reject unpublished, mismatched or incomplete GitHub release metadata."""
    if release.get("tagName") != downloads["tag"] or release.get("isDraft") is not False:
        raise ValueError("Desktop downloads require the exact published release")
    if release.get("url") != downloads["release_url"]:
        raise ValueError("Desktop release URL differs from the documented download source")
    expected = {asset["name"]: asset["url"] for asset in downloads["assets"]}
    expected["SHA256SUMS"] = downloads["checksum_url"]
    assets = release.get("assets", [])
    for name, url in expected.items():
        matches = [asset for asset in assets if asset.get("name") == name]
        if len(matches) != 1:
            raise ValueError(f"Desktop release must contain exactly one asset: {name}")
        asset = matches[0]
        if asset.get("state") != "uploaded" or asset.get("size", 0) <= 0:
            raise ValueError(f"Desktop release asset is not uploaded and nonempty: {name}")
        if asset.get("url") != url:
            raise ValueError(f"Desktop release asset URL differs from the download link: {name}")


def verify_desktop_downloads(root=ROOT):
    """Read only GitHub release metadata; no installer or checksum bytes are fetched."""
    downloads = desktop_downloads(root)
    release = json.loads(subprocess.check_output([
        "gh", "release", "view", downloads["tag"], "--repo", REPOSITORY,
        "--json", "tagName,isDraft,url,assets",
    ], cwd=root, text=True))
    inspect_release_assets(downloads, release)
    print(f"Desktop downloads verified: {downloads['tag']}, 3 installers and SHA256SUMS", flush=True)
    return downloads


if __name__ == "__main__":
    try:
        verify_desktop_downloads()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Desktop download verification failed: {error}", file=sys.stderr)
        sys.exit(1)
