"""Derive desktop download links and verify published release assets without downloading them."""

import json
import os
from pathlib import Path
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


def container_downloads(root=ROOT, environment=None):
    """Only exact publication-event assets establish browser availability."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    project = tomllib.loads((root / "website/zensical.toml").read_text())["project"]
    tag = project["extra"]["desktop_release"]
    version = release_version(tag, historical=True)
    if not version:
        raise ValueError("Browser downloads require the documented core release")
    guide = root / "website/docs/start/containerlab.md"
    if guide.read_text().count(CONTAINER_MARKER) != 1:
        raise ValueError("Containerlab guide requires exactly one browser download marker")
    base = f"https://github.com/{REPOSITORY}/releases"
    result = {"available": False, "tag": tag, "version": version,
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
    assets = release.get("assets", [])
    if not isinstance(assets, list):
        raise ValueError("Browser release assets must be a list")
    if not any(isinstance(asset, dict) and asset.get("name") in
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
    result.update({"available": True, "checksum_url": checksum_url})
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
