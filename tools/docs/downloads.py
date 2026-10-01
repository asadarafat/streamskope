"""Derive desktop download links and verify published release assets without downloading them."""

import json
from pathlib import Path
import subprocess
import sys
import tomllib
from urllib.parse import quote

if not __package__:
    # Pages invokes this file directly; module imports use tools/ as their root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from docs.publication import release_version


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
