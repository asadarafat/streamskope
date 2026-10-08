from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs.qualification import write_qualification


class QualificationTests(unittest.TestCase):
    def test_records_actual_routes_and_explicit_media_skip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / ".artifacts/website"
            directory.mkdir(parents=True)
            site = root / "dist/site"
            site.mkdir(parents=True)
            (site / "index.html").write_text("<h1>Home</h1>")
            now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            (directory / "browser-checks.json").write_text(json.dumps({
                "schemaVersion": 1, "outcome": "passed", "routes": 17, "media": "skipped",
                "startedAt": now, "completedAt": now,
            }))
            write_qualification(root, now, False, "a" * 64)
            report = json.loads((directory / "qualification.json").read_text())
            self.assertEqual(report["routes"], 17)
            self.assertEqual(report["htmlPages"], 1)
            self.assertEqual(report["media"]["reason"], "unchanged-media-inputs")
            self.assertEqual(len(report["browserSha256"]), 64)

    def test_rejects_stale_or_incomplete_browser_evidence(self):
        for outcome, completed in (("failed", "2026-01-01T00:00:00.000Z"), ("passed", "2025-01-01T00:00:00.000Z")):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                directory = root / ".artifacts/website"
                directory.mkdir(parents=True)
                (directory / "browser-checks.json").write_text(json.dumps({
                    "schemaVersion": 1, "outcome": outcome, "routes": 17, "media": "passed",
                    "startedAt": completed, "completedAt": completed,
                }))
                with self.assertRaises(ValueError):
                    write_qualification(root, "2026-01-01T00:00:00.000Z", True, "a" * 64)
                self.assertFalse((directory / "qualification.json").exists())
