"""Pin approved remote videos; cache verified masters outside source and Pages."""
import hashlib
import json
from pathlib import Path
import re
import shutil
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]


def video_assets(root=ROOT):
    data = json.loads((Path(root) / "website/media.json").read_text())
    if data.get("schemaVersion") != 1 or not data.get("assets"):
        raise ValueError("Unsupported or empty video manifest")
    seen = set()
    names = set()
    for asset in data["assets"]:
        path = asset["path"]
        expected = f'https://github.com/asadarafat/streamskope/releases/download/{data["release"]}/{asset["name"]}'
        if (not re.fullmatch(r"assets/(?:skope/)?[a-z0-9-]+\.mp4", path)
                or Path(path).name != asset["name"] or path in seen or asset["name"] in names
                or asset["url"] != expected or not data["release"].startswith("documentation-media/")
                or not re.fullmatch(r"[a-f0-9]{64}", asset["sha256"])
                or type(asset["bytes"]) is not int or not 0 < asset["bytes"] < 2 * 1024**3):
            raise ValueError("Invalid video asset identity")
        seen.add(path)
        names.add(asset["name"])
    return data["assets"]


def fetch_videos(root=ROOT, destination=None):
    root = Path(root)
    for asset in video_assets(root):
        cache = root / ".cache/docs-media" / asset["sha256"] / asset["name"]
        def verified(file):
            return (file.is_file() and file.stat().st_size == asset["bytes"]
                    and hashlib.sha256(file.read_bytes()).hexdigest() == asset["sha256"])
        if not verified(cache):
            cache.parent.mkdir(parents=True, exist_ok=True)
            temporary = cache.with_suffix(".download")
            try:
                with urlopen(asset["url"], timeout=60) as response, temporary.open("wb") as output:
                    total = 0
                    while chunk := response.read(1024 * 1024):
                        total += len(chunk)
                        if total > asset["bytes"]:
                            raise ValueError("Video exceeds its approved size")
                        output.write(chunk)
                if not verified(temporary):
                    raise ValueError("Downloaded video does not match its approved digest")
                temporary.replace(cache)
            finally:
                temporary.unlink(missing_ok=True)
        if destination is not None:
            target = Path(destination) / asset["name"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(cache, target)
    print(f"Verified {len(video_assets(root))} approved video masters")


def inspect_video_references(site, root=ROOT):
    assets = video_assets(root)
    content = "\n".join(file.read_text() for file in Path(site).rglob("*")
                        if file.suffix in {".html", ".js"})
    for asset in assets:
        if asset["url"] not in content:
            raise ValueError(f'Missing pinned video reference: {asset["path"]}')
    if any(Path(site).rglob("*.mp4")):
        raise ValueError("Video masters must not enter the Pages artifact")
