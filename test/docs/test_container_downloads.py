"""Browser availability follows exact publication assets, never development version."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import downloads


class ContainerDownloadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "website/docs/start").mkdir(parents=True)
        (self.root / "website/zensical.toml").write_text('[project.extra]\ndesktop_release = "v0.10.0"\n')
        (self.root / "website/docs/start/containerlab.md").write_text(downloads.CONTAINER_MARKER)
        names = [
            "StreamSkope-0.10.0-container-linux-amd64.tar.gz",
            "StreamSkope-0.10.0-container-linux-arm64.tar.gz",
            "streamskope-0.10.0.clab.yml", "streamskope-0.10.0-offline.clab.yml",
            "streamskope-0.10.0-container.json", "SHA256SUMS",
        ]
        self.release = {"tag_name": "v0.10.0", "assets": [{
            "name": name, "size": 10, "state": "uploaded",
            "browser_download_url": f"https://github.com/asadarafat/streamskope/releases/download/v0.10.0/{name}",
        } for name in names]}

    def published(self):
        with patch.object(downloads, "published_release", return_value=self.release) as guard:
            result = downloads.container_downloads(self.root, {"STREAMSKOPE_DOCS_PUBLISH": "1"})
            guard.assert_called_once()
            return result

    def test_source_never_asserts_a_browser_download(self):
        with patch.object(downloads, "published_release") as guard:
            result = downloads.container_downloads(self.root, {})
        self.assertFalse(result["available"])
        self.assertEqual(result["assets"], [])
        guard.assert_not_called()

    def test_exact_complete_release_is_available(self):
        result = self.published()
        self.assertTrue(result["available"])
        self.assertEqual(len(result["assets"]), 5)
        self.assertTrue(result["registry_delivery"])
        self.assertTrue(result["checksum_url"].endswith("v0.10.0/SHA256SUMS"))

    def test_historical_desktop_only_release_does_not_invent_images(self):
        self.release["assets"] = []
        self.assertFalse(self.published()["available"])

    def test_partial_duplicate_wrong_url_and_empty_assets_fail_closed(self):
        original = json.loads(json.dumps(self.release))
        mutations = [
            lambda: self.release["assets"].pop(0),
            lambda: self.release["assets"].pop(3),
            lambda: self.release["assets"].append(self.release["assets"][0]),
            lambda: self.release["assets"][0].update(browser_download_url="https://example.test/renamed.tar.gz"),
            lambda: self.release["assets"][1].update(size=0),
            lambda: self.release["assets"][2].update(state="new"),
            lambda: self.release["assets"].pop(),
        ]
        for mutate in mutations:
            self.release = json.loads(json.dumps(original))
            mutate()
            with self.assertRaises(ValueError):
                self.published()

    def test_selected_release_mismatch_is_rejected(self):
        self.release["tag_name"] = "v0.9.2"
        with self.assertRaises(ValueError):
            self.published()

    def test_historical_archive_release_retains_its_exact_topology(self):
        config = self.root / "website/zensical.toml"
        config.write_text('[project.extra]\ndesktop_release = "v0.9.3"\n')
        self.release = json.loads(json.dumps(self.release).replace("0.10.0", "0.9.3"))
        self.release["assets"] = [asset for asset in self.release["assets"]
                                  if "-offline.clab.yml" not in asset["name"]]
        result = self.published()
        self.assertTrue(result["available"])
        self.assertFalse(result["registry_delivery"])
        self.assertEqual(len(result["assets"]), 4)


if __name__ == "__main__":
    unittest.main()
