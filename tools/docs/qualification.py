"""Record successful documentation qualification without parsing console output."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def write_qualification(root: Path, started: str, media: bool, fingerprint: str):
    directory = root / ".artifacts/website"
    contents = (directory / "browser-checks.json").read_bytes()
    browser = json.loads(contents)
    completed = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if (
        browser.get("schemaVersion") != 1 or browser.get("outcome") != "passed"
        or not isinstance(browser.get("routes"), int) or browser["routes"] < 1
        or browser.get("media") != ("passed" if media else "skipped")
        or not started <= browser.get("startedAt", "") <= browser.get("completedAt", "") <= completed
    ):
        raise ValueError("Browser evidence is incomplete, stale or from a different documentation run")
    report = {
        "schemaVersion": 1, "outcome": "passed", "startedAt": started, "completedAt": completed,
        "htmlPages": sum(1 for _ in (root / "dist/site").rglob("*.html")),
        "routes": browser["routes"], "browserSha256": hashlib.sha256(contents).hexdigest(),
        "media": {"outcome": browser["media"], "fingerprint": fingerprint,
                  **({} if media else {"reason": "unchanged-media-inputs"})},
    }
    (directory / "qualification.json").write_text(json.dumps(report, indent=2) + "\n")
