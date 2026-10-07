"""Browser availability follows exact publication assets, never development version."""
import json
from html.parser import HTMLParser
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from jinja2 import Environment, FileSystemLoader, select_autoescape

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import downloads


class RenderedInstallation(HTMLParser):
    def __init__(self, rendered):
        super().__init__()
        self.commands = []
        self.links = []
        self.sections = []
        self.scripts = []
        self.in_code = False
        self.feed(rendered)

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "code":
            self.in_code = True
            self.commands.append("")
        if tag == "a":
            self.links.append(values.get("href"))
        if tag == "section":
            self.sections.append(values)
        if tag == "script":
            self.scripts.append(values)

    def handle_endtag(self, tag):
        if tag == "code":
            self.in_code = False

    def handle_data(self, data):
        if self.in_code:
            self.commands[-1] += data


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

    def modern(self):
        (self.root / "website/docs/start/containerlab.md").write_text(
            "---\nbrowser_installer: true\n---\n" + downloads.BROWSER_INSTALLER_MARKER)
        guide = self.root / "website/docs/guide"
        guide.mkdir(exist_ok=True)
        (guide / "browser-host.md").write_text(
            "---\ncontainer_downloads: true\n---\n" + downloads.CONTAINER_MARKER)
        self.release["assets"].append({
            "name": downloads.BROWSER_INSTALLER_NAME, "size": 30_000, "state": "uploaded",
            "browser_download_url": "https://github.com/asadarafat/streamskope/releases/download/"
                                    f"v0.10.0/{downloads.BROWSER_INSTALLER_NAME}",
        })

    @staticmethod
    def render(downloads_context, partial="browser-installer.html", published=True):
        overrides = Path(__file__).resolve().parents[2] / "website/overrides"
        environment = Environment(loader=FileSystemLoader(overrides),
                                  autoescape=select_autoescape(["html"]))
        environment.filters["url"] = lambda value: "/streamskope/" + value
        return environment.get_template("partials/" + partial).render(config={"extra": {
            "container_downloads": downloads_context,
            "documentation": {"status": "Published documentation" if published else
                                         "Development documentation"},
        }})

    def test_source_never_asserts_a_browser_download(self):
        with patch.object(downloads, "published_release") as guard:
            result = downloads.container_downloads(self.root, {})
        self.assertFalse(result["available"])
        self.assertFalse(result["installer_available"])
        self.assertEqual(result["assets"], [])
        guard.assert_not_called()

    def test_exact_complete_release_is_available(self):
        result = self.published()
        self.assertTrue(result["available"])
        self.assertEqual(len(result["assets"]), 5)
        self.assertTrue(result["registry_delivery"])
        self.assertFalse(result["installer_available"])
        self.assertNotIn("installer_url", result)
        self.assertNotIn("install_command", result)
        self.assertTrue(result["checksum_url"].endswith("v0.10.0/SHA256SUMS"))

    def test_modern_source_preview_does_not_assert_installer_availability(self):
        self.modern()
        with patch.object(downloads, "published_release") as guard:
            result = downloads.container_downloads(self.root, {})
        self.assertFalse(result["available"])
        self.assertFalse(result["installer_available"])
        self.assertEqual(result["assets"], [])
        self.assertNotIn("installer_url", result)
        self.assertNotIn("install_command", result)
        guard.assert_not_called()
        rendered = self.render(result, published=False)
        self.assertEqual(RenderedInstallation(rendered).commands, [])
        self.assertIn("source preview does not verify", rendered)
        self.assertNotIn("not published", rendered)

    def test_modern_publication_renders_one_latest_copyable_command(self):
        self.modern()
        result = self.published()
        self.assertTrue(result["installer_available"])
        self.assertEqual(len(result["assets"]), 6)
        url = "https://github.com/asadarafat/streamskope/releases/download/" \
              "v0.10.0/install-browser-workbench.sh"
        latest_url = "https://github.com/asadarafat/streamskope/releases/latest/download/" \
                     "install-browser-workbench.sh"
        self.assertEqual(result["installer_url"], url)
        self.assertEqual(result["install_command"], f"curl -fsSL {latest_url} | sudo -E bash")
        rendered = self.render(result)
        parsed = RenderedInstallation(rendered)
        self.assertEqual(parsed.commands, [f"curl -fsSL {latest_url} | sudo -E bash"])
        self.assertEqual(parsed.sections, [{
            "class": "sk-browser-installer", "aria-label": "Browser workbench installation",
            "data-desktop-release": "v0.10.0",
        }])
        self.assertIn('class="highlight"', rendered)
        self.assertNotIn("<table", rendered)
        self.assertIn("latest stable desktop release", rendered)
        self.assertIn("URL printed by the installer", rendered)
        self.assertIn("/streamskope/guide/browser-host/", parsed.links)

    def test_quick_install_command_stays_the_same_when_publication_advances(self):
        self.modern()
        original = self.published()
        config = self.root / "website/zensical.toml"
        config.write_text('[project.extra]\ndesktop_release = "v0.10.1"\n')
        self.release = json.loads(json.dumps(self.release).replace("0.10.0", "0.10.1"))
        newer = self.published()
        self.assertEqual(original["install_command"], newer["install_command"])
        self.assertNotEqual(original["installer_url"], newer["installer_url"])
        self.assertIn("/download/v0.10.1/", newer["installer_url"])

    def test_generated_command_is_escaped_as_text_not_executable_markup(self):
        self.modern()
        result = self.published()
        command = "curl 'https://example.test/?a=1&b=<script>alert(1)</script>' | sudo -E bash"
        result["install_command"] = command
        rendered = self.render(result)
        parsed = RenderedInstallation(rendered)
        self.assertEqual(parsed.commands, [command])
        self.assertEqual(parsed.scripts, [])
        self.assertIn("&lt;script&gt;", rendered)
        self.assertIn("&amp;b=", rendered)

    def test_historical_manual_assets_render_no_invented_installer(self):
        result = self.published()
        rendered = self.render(result)
        self.assertEqual(RenderedInstallation(rendered).commands, [])
        self.assertIn("no verified one-command installer", rendered)
        manual = self.render(result, "container-downloads.html")
        self.assertNotIn(downloads.BROWSER_INSTALLER_NAME, manual)
        for asset in result["assets"]:
            self.assertIn(asset["url"], RenderedInstallation(manual).links)

    def test_manual_preview_does_not_misrepresent_released_assets_as_unpublished(self):
        result = downloads.container_downloads(self.root, {})
        rendered = self.render(result, "container-downloads.html", published=False)
        self.assertIn("source preview does not verify", rendered)
        self.assertNotIn("no verified browser assets", rendered)
        self.assertNotIn("source build instructions", rendered)

    def test_modern_source_cannot_silently_downgrade_to_manual_or_desktop_only(self):
        self.modern()
        original = json.loads(json.dumps(self.release))
        for assets in [[], [asset for asset in original["assets"]
                            if asset["name"] != downloads.BROWSER_INSTALLER_NAME]]:
            with self.subTest(assets=len(assets)):
                self.release["assets"] = assets
                with self.assertRaises(ValueError):
                    self.published()

    def test_modern_installer_and_checksum_metadata_fail_closed(self):
        self.modern()
        original = json.loads(json.dumps(self.release))
        for name in [downloads.BROWSER_INSTALLER_NAME, "SHA256SUMS"]:
            for change in ["missing", "duplicate", "empty", "wrong-url", "pending", "boolean-size"]:
                with self.subTest(asset=name, change=change):
                    self.release = json.loads(json.dumps(original))
                    asset = next(value for value in self.release["assets"] if value["name"] == name)
                    if change == "missing":
                        self.release["assets"].remove(asset)
                    elif change == "duplicate":
                        self.release["assets"].append(dict(asset))
                    elif change == "empty":
                        asset["size"] = 0
                    elif change == "boolean-size":
                        asset["size"] = True
                    elif change == "wrong-url":
                        asset["browser_download_url"] = (
                            f"https://github.com/asadarafat/streamskope/releases/latest/download/{name}")
                    else:
                        asset["state"] = "new"
                    with self.assertRaises(ValueError):
                        self.published()

    def test_modern_markers_require_the_reviewed_quickstart_and_manual_page(self):
        self.modern()
        quick = self.root / "website/docs/start/containerlab.md"
        manual = self.root / "website/docs/guide/browser-host.md"
        original = {quick: quick.read_text(), manual: manual.read_text()}
        extra = self.root / "website/docs/guide/extra.md"
        for change in ["quick-marker", "quick-flag", "duplicate-quick", "manual-marker", "manual-flag", "duplicate-manual", "extra-quick-page", "extra-manual-page"]:
            with self.subTest(change=change):
                for page, source in original.items():
                    page.write_text(source)
                extra.unlink(missing_ok=True)
                if change == "quick-marker":
                    quick.write_text(original[quick].replace(downloads.BROWSER_INSTALLER_MARKER, ""))
                elif change == "quick-flag":
                    quick.write_text(original[quick].replace("browser_installer: true", ""))
                elif change == "duplicate-quick":
                    quick.write_text(original[quick] + downloads.BROWSER_INSTALLER_MARKER)
                elif change == "manual-marker":
                    manual.write_text(original[manual].replace(downloads.CONTAINER_MARKER, ""))
                elif change == "manual-flag":
                    manual.write_text(original[manual].replace("container_downloads: true", ""))
                elif change == "duplicate-manual":
                    manual.write_text(original[manual] + downloads.CONTAINER_MARKER)
                elif change == "extra-quick-page":
                    extra.write_text(downloads.BROWSER_INSTALLER_MARKER)
                else:
                    extra.write_text(downloads.CONTAINER_MARKER)
                with patch.object(downloads, "published_release") as publication:
                    with self.assertRaises(ValueError):
                        downloads.container_downloads(self.root, {})
                    publication.assert_not_called()

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
