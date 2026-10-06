"""Availability is a catalog observation, never inferred from source versions."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import plugins


class PluginAvailabilityTests(unittest.TestCase):
    identity = {"revision": "a" * 40, "desktop_release": "v0.7.1"}

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        file = self.root / "plugins/eda/manifest.json"
        file.parent.mkdir(parents=True)
        file.write_text(json.dumps({
            "id": "streamskope.eda", "name": "EDA Connector", "version": "0.0.0-dev", "apiVersion": 4,
            "compatibility": {"streamskope": {"minimum": "0.4.0", "maximumExclusive": "0.8.0"},
                              "target": {"system": "eda", "minimum": "26.8.2", "maximum": "26.8.2"}},
        }))
        self.snapshot = {**self.identity, "checked_at": "2026-10-04T12:00:00+00:00", "packages": [
            {"id": "streamskope.eda", "version": "v0.1.0+build.1--eda-26.8.2-26.8.2--r1",
             "api": 3, "sha256": "b" * 64, "release_tag": "v0.1.0+build.1",
             "release_url": plugins.RELEASE_ROOT + "/tag/v0.1.0%2Bbuild.1", "portable": None,
             "compatibility": {"streamskope": {"minimum": "v0.1.0+build.1"},
                               "target": {"system": "eda", "minimum": "26.8.2", "maximum": "26.8.2"}}}]}

    def published(self, snapshot=None):
        path = self.root / plugins.SNAPSHOT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot or self.snapshot))
        with patch.object(plugins, "publication_identity", return_value=self.identity):
            return plugins.plugin_context(self.root, published=True)[0]

    def test_preview_does_not_query_or_claim_publication(self):
        with patch.object(plugins.subprocess, "check_output", side_effect=AssertionError("network")):
            row = plugins.plugin_context(self.root)[0]
        self.assertEqual(row["availability"], "unchecked")
        self.assertEqual(row["portable_availability"], "unchecked")
        self.assertNotIn("published_version", row)

    def test_source_development_and_published_legacy_version_remain_distinct(self):
        row = self.published()
        self.assertEqual((row["version"], row["api"]), ("0.0.0-dev", 4))
        self.assertEqual(row["published_api"], 3)
        self.assertEqual(row["published_version"], self.snapshot["packages"][0]["version"])
        self.assertEqual(row["availability"], "published")
        self.assertEqual(row["portable_availability"], "unavailable")
        self.assertNotIn("portable_url", row)

    def portable_snapshot(self):
        snapshot = deepcopy(self.snapshot)
        package = snapshot["packages"][0]
        package.update(version="0.1.0", api=4, release_tag="plugins/eda/v0.1.0",
                       release_url=plugins.RELEASE_ROOT + "/tag/plugins/eda/v0.1.0")
        package["compatibility"]["streamskope"] = {"minimum": "0.4.0", "maximumExclusive": "1.0.0"}
        name = "streamskope-eda-portable-v0.1.0.skope-plugin"
        package["portable"] = {"name": name, "sha256": "c" * 64, "size": 1000,
                               "url": plugins.RELEASE_ROOT + "/download/plugins/eda/v0.1.0/" + name,
                               "publisher": "StreamSkope", "publisher_key_id": "streamskope-test-key"}
        return snapshot

    def test_verified_portable_exposes_its_exact_link_and_published_requirements(self):
        snapshot = self.portable_snapshot()
        row = self.published(snapshot)
        self.assertEqual(row["portable_availability"], "published")
        self.assertEqual(row["portable_url"], snapshot["packages"][0]["portable"]["url"])
        self.assertEqual(row["portable_sha256"], "c" * 64)
        self.assertEqual(row["published_maximum_host_exclusive"], "1.0.0")
        self.assertEqual(row["maximum_host_exclusive"], "0.8.0")
        self.assertEqual(row["portable_publisher"], "StreamSkope")

    def test_unsafe_or_cross_release_links_cannot_be_rendered(self):
        for field, value in (("url", "https://example.com/plugin.skope-plugin"),
                             ("url", plugins.RELEASE_ROOT + "/download/plugins/eda/v0.2.0/streamskope-eda-portable-v0.1.0.skope-plugin"),
                             ("url", plugins.RELEASE_ROOT + "/download/plugins/eda/v0.1.0/streamskope-eda-portable-v0.1.0.skope-plugin?redirect=evil"),
                             ("name", "streamskope-nsp-portable-v0.1.0.skope-plugin"),
                             ("sha256", "unverified"), ("size", 0), ("size", True),
                             ("publisher_key_id", "untrusted key"), ("publisher", "publisher\nvalue")):
            with self.subTest(field=field, value=value):
                snapshot = self.portable_snapshot()
                snapshot["packages"][0]["portable"][field] = value
                with self.assertRaises(ValueError):
                    self.published(snapshot)
        for field, value in (("release_tag", "plugins/nsp/v0.1.0"),
                             ("release_url", plugins.RELEASE_ROOT + "/tag/plugins/eda/v0.2.0"),
                             ("release_url", "https://github.com.evil.example/asadarafat/streamskope/releases/tag/plugins/eda/v0.1.0")):
            snapshot = self.portable_snapshot()
            snapshot["packages"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.published(snapshot)

    def test_snapshot_requires_explicit_portable_state_and_valid_compatibility(self):
        snapshot = self.portable_snapshot()
        del snapshot["packages"][0]["portable"]
        with self.assertRaisesRegex(ValueError, "portable availability"):
            self.published(snapshot)
        for compatibility in (None, {"streamskope": {}, "target": {}},
                              {"streamskope": {"minimum": "0.4.0"},
                               "target": {"system": "eda", "minimum": "26.8.2", "maximum": "26.8.2"}}):
            snapshot = self.portable_snapshot()
            snapshot["packages"][0]["compatibility"] = compatibility
            with self.subTest(compatibility=compatibility), self.assertRaises(ValueError):
                self.published(snapshot)

    def test_empty_verified_catalog_means_no_compatible_package_at_that_time(self):
        row = self.published({**self.snapshot, "packages": []})
        self.assertEqual(row["availability"], "unavailable")
        self.assertEqual(row["portable_availability"], "unavailable")
        self.assertEqual(row["checked_at"], "2026-10-04 12:00 UTC")

    def test_missing_stale_or_invalid_snapshot_cannot_claim_availability(self):
        with self.assertRaisesRegex(ValueError, "verified availability"):
            plugins.plugin_context(self.root, published=True)
        for field, value in (("revision", "c" * 40), ("desktop_release", "v0.7.0"),
                             ("checked_at", "yesterday"), ("packages", {})):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.published({**self.snapshot, field: value})
        bad = deepcopy(self.snapshot)
        bad["packages"][0]["sha256"] = "unverified"
        with self.assertRaises(ValueError):
            self.published(bad)

    def test_catalog_error_is_not_converted_into_package_unavailability(self):
        with patch.object(plugins, "publication_identity", return_value=self.identity), \
                patch.object(plugins.subprocess, "check_output", side_effect=OSError("offline")):
            with self.assertRaises(OSError):
                plugins.capture_publications(self.root)
        self.assertFalse((self.root / plugins.SNAPSHOT).exists())

    def test_successful_capture_preserves_verified_portable_metadata(self):
        snapshot = self.portable_snapshot()
        with patch.object(plugins, "publication_identity", return_value=self.identity), \
                patch.object(plugins.subprocess, "check_output", return_value=json.dumps(snapshot["packages"])):
            captured = plugins.capture_publications(self.root)
        self.assertEqual(captured["packages"], snapshot["packages"])
        self.assertEqual(self.published(captured)["portable_availability"], "published")

    def test_offline_guide_requires_the_visible_table_marker_and_metadata(self):
        file = self.root / "website/docs/plugins/offline.md"
        file.parent.mkdir(parents=True)
        for content in ("# Offline", "plugin_portable_downloads: true\n",
                        plugins.PORTABLE_MARKER, "plugin_portable_downloads: true\n" + plugins.PORTABLE_MARKER * 2):
            file.write_text(content)
            with self.subTest(content=content), self.assertRaisesRegex(ValueError, "portable download marker"):
                plugins.plugin_context(self.root)
        file.write_text("---\nplugin_portable_downloads: true\n---\n" + plugins.PORTABLE_MARKER)
        self.assertEqual(plugins.plugin_context(self.root)[0]["portable_availability"], "unchecked")


if __name__ == "__main__":
    unittest.main()
