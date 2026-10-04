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
            "id": "streamskope.eda", "name": "EDA Capture", "version": "0.0.0-dev", "apiVersion": 4,
            "compatibility": {"streamskope": {"minimum": "0.4.0", "maximumExclusive": "0.8.0"},
                              "target": {"system": "eda", "minimum": "26.8.2", "maximum": "26.8.2"}},
        }))
        self.snapshot = {**self.identity, "checked_at": "2026-10-04T12:00:00+00:00", "packages": [
            {"id": "streamskope.eda", "version": "v0.1.0+build.1--eda-26.8.2-26.8.2--r1",
             "api": 3, "sha256": "b" * 64}]}

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
        self.assertNotIn("published_version", row)

    def test_source_development_and_published_legacy_version_remain_distinct(self):
        row = self.published()
        self.assertEqual((row["version"], row["api"]), ("0.0.0-dev", 4))
        self.assertEqual(row["published_api"], 3)
        self.assertEqual(row["published_version"], self.snapshot["packages"][0]["version"])
        self.assertEqual(row["availability"], "published")

    def test_empty_verified_catalog_means_no_compatible_package_at_that_time(self):
        row = self.published({**self.snapshot, "packages": []})
        self.assertEqual(row["availability"], "unavailable")
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


if __name__ == "__main__":
    unittest.main()
