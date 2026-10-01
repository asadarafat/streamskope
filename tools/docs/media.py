"""Select expensive media playback without skipping routine documentation checks."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]

def media_inputs(path):
    return (path.startswith(("website/docs/intro/", "website/docs/launch/", "website/docs/assets/",
                             "website/overrides/", "tools/docs/"))
            or path in {"website/docs/index.md", "website/requirements.txt", "website/zensical.toml",
                        "package-lock.json", "tools/docs.py", ".github/workflows/launch-video.yml",
                        "src/platform/ui/colorContract.ts", "src/platform/ui/typographyContract.ts"})


def media_selection(root=ROOT, environment=None):
    """Unknown CI history fails safe to full playback; prose-only changes skip it."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    files = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root,
    ).decode().split("\0")
    digest = hashlib.sha256()
    for name in sorted(set(filter(media_inputs, filter(None, files)))):
        file = root / name
        digest.update(name.encode())
        digest.update(file.read_bytes() if file.is_file() else b"deleted")
    fingerprint = digest.hexdigest()
    if environment.get("CI") == "true":
        try:
            event = json.loads(Path(environment["GITHUB_EVENT_PATH"]).read_text())
            if environment.get("GITHUB_EVENT_NAME") == "pull_request":
                base = event["pull_request"]["base"]["sha"]
            elif environment.get("GITHUB_EVENT_NAME") == "push":
                base = event["before"]
            else:
                return True, fingerprint
            changed = subprocess.check_output(
                ["git", "diff", "--name-only", base, "HEAD"], cwd=root, stderr=subprocess.DEVNULL,
            ).decode().splitlines()
            return any(media_inputs(path) for path in changed), fingerprint
        except (KeyError, OSError, ValueError, subprocess.CalledProcessError):
            return True, fingerprint
    marker = root / ".cache/docs-media-qualified"
    return not marker.exists() or marker.read_text() != fingerprint, fingerprint
