"""Regression checks for the exact desktop release offered on the installation page."""

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import downloads


class DesktopDownloadTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "website/docs/start").mkdir(parents=True)
        self.installation = self.root / "website/docs/start/installation.md"
        self.installation.write_text("## Download\n\n" + downloads.DOWNLOAD_MARKER)
        (self.root / "package.json").write_text('{"version":"9.8.7"}')
        self.write_release("v0.1.0+build.4")

    def write_release(self, tag):
        (self.root / "website/zensical.toml").write_text(
            f'[project.extra]\ndesktop_release = "{tag}"\n')

    def metadata(self):
        links = downloads.desktop_downloads(self.root)
        assets = [{**asset, "state": "uploaded", "size": 123} for asset in links["assets"]]
        assets.append({"name": "SHA256SUMS", "url": links["checksum_url"],
                       "state": "uploaded", "size": 308})
        return links, {"tagName": links["tag"], "isDraft": False,
                       "url": links["release_url"], "assets": assets}

    def test_uses_documented_build_and_app_version_not_development_version(self):
        links = downloads.desktop_downloads(self.root)
        self.assertEqual(links["tag"], "v0.1.0+build.4")
        self.assertEqual(links["version"], "0.1.0")
        self.assertEqual(links["notes_path"], "releases/v0.1.0+build.4/")
        expected_root = "https://github.com/asadarafat/streamskope/releases/download/v0.1.0%2Bbuild.4/"
        self.assertEqual([(asset["platform"], asset["name"], asset["url"]) for asset in links["assets"]], [
            ("macOS, Apple Silicon", "StreamSkope-0.1.0-darwin-arm64.dmg",
             expected_root + "StreamSkope-0.1.0-darwin-arm64.dmg"),
            ("Windows x64", "StreamSkope-0.1.0-win32-x64-Setup.exe",
             expected_root + "StreamSkope-0.1.0-win32-x64-Setup.exe"),
            ("Linux x64", "StreamSkope-0.1.0-linux-x64.AppImage",
             expected_root + "StreamSkope-0.1.0-linux-x64.AppImage"),
        ])
        self.assertEqual(links["checksum_url"], expected_root + "SHA256SUMS")

    def test_changing_documented_release_updates_every_download(self):
        for tag, version, encoded in (("v0.1.0+build.5", "0.1.0", "v0.1.0%2Bbuild.5"),
                                      ("v1.2.3", "1.2.3", "v1.2.3"),
                                      ("v0.2.0-rc.1", "0.2.0-rc.1", "v0.2.0-rc.1")):
            with self.subTest(tag=tag):
                self.write_release(tag)
                links = downloads.desktop_downloads(self.root)
                self.assertEqual(links["tag"], tag)
                self.assertEqual(links["version"], version)
                self.assertTrue(links["release_url"].endswith("/" + encoded))
                self.assertEqual(links["notes_path"], f"releases/{tag}/")
                self.assertTrue(links["checksum_url"].endswith(f"/{encoded}/SHA256SUMS"))
                for asset in links["assets"]:
                    self.assertIn(f"/{encoded}/StreamSkope-{version}-", asset["url"])

    def test_rejects_floating_or_malformed_release_identity(self):
        for tag in ("latest", "v0.1.0+build.0", "0.1.0", "v0.1.0/other", "v0.01.0",
                    "v0.2.0-rc.01", "v0.2.0+commit.deadbeef"):
            with self.subTest(tag=tag):
                self.write_release(tag)
                with self.assertRaisesRegex(ValueError, "exact documented release"):
                    downloads.desktop_downloads(self.root)

    def test_requires_exactly_one_rendering_location(self):
        for source in ("## Download", downloads.DOWNLOAD_MARKER * 2):
            with self.subTest(source=source):
                self.installation.write_text(source)
                with self.assertRaisesRegex(ValueError, "exactly one desktop download marker"):
                    downloads.desktop_downloads(self.root)

    def test_accepts_complete_release_with_extra_plugin_assets(self):
        links, release = self.metadata()
        release["assets"].append({"name": "streamskope-nsp.skope-plugin"})
        downloads.inspect_release_assets(links, release)

    def test_rejects_draft_wrong_release_and_missing_asset(self):
        links, release = self.metadata()
        cases = []
        for field, value in (("isDraft", True), ("tagName", "v0.1.0+build.3"),
                             ("url", "https://example.invalid/release")):
            changed = deepcopy(release)
            changed[field] = value
            cases.append(changed)
        for index in range(4):
            changed = deepcopy(release)
            changed["assets"].pop(index)
            cases.append(changed)
        for changed in cases:
            with self.subTest(release=changed), self.assertRaises(ValueError):
                downloads.inspect_release_assets(links, changed)

    def test_rejects_unusable_or_misdirected_assets(self):
        links, release = self.metadata()
        for index in range(4):
            for field, value in (("state", "new"), ("size", 0),
                                 ("url", "https://example.invalid/download")):
                changed = deepcopy(release)
                changed["assets"][index][field] = value
                with self.subTest(index=index, field=field), self.assertRaises(ValueError):
                    downloads.inspect_release_assets(links, changed)
        release["assets"].append(release["assets"][0])
        with self.assertRaisesRegex(ValueError, "exactly one asset"):
            downloads.inspect_release_assets(links, release)

    def test_remote_verification_fetches_metadata_for_the_documented_tag(self):
        links, release = self.metadata()
        with patch.object(downloads.subprocess, "check_output", return_value=json.dumps(release)) as fetch:
            self.assertEqual(downloads.verify_desktop_downloads(self.root), links)
        fetch.assert_called_once_with([
            "gh", "release", "view", "v0.1.0+build.4", "--repo", "asadarafat/streamskope",
            "--json", "tagName,isDraft,url,assets",
        ], cwd=self.root, text=True)

    def test_pages_can_invoke_download_verification_as_a_standalone_script(self):
        # A child process has no tools/ sys.path inherited from this test module.
        # Mock only gh's response, preserving the exact workflow's script entrypoint.
        script = Path(downloads.__file__).resolve()
        links = downloads.desktop_downloads(downloads.ROOT)
        release = {"tagName": links["tag"], "isDraft": False, "url": links["release_url"],
                   "assets": [{**asset, "state": "uploaded", "size": 123}
                              for asset in links["assets"]] + [
                       {"name": "SHA256SUMS", "url": links["checksum_url"],
                        "state": "uploaded", "size": 100}]}
        (self.root / "sitecustomize.py").write_text(
            "import subprocess\n"
            f"subprocess.check_output = lambda *args, **kwargs: {json.dumps(release)!r}\n")
        result = subprocess.run([sys.executable, str(script)], cwd=self.root,
                                env={**os.environ, "PYTHONPATH": str(self.root)},
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"Desktop downloads verified: {links['tag']}", result.stdout)


if __name__ == "__main__":
    unittest.main()
