"""Behavioral release selection checks independent of workflow YAML spelling."""

from copy import deepcopy
import json
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import publication


class StablePublicationTests(unittest.TestCase):
    revision = "a" * 40

    def release(self, tag, identity=10, **fields):
        return {"tag_name": tag, "id": identity, "draft": False, "prerelease": False,
                "immutable": True, "published_at": "2026-10-04T06:22:07Z", **fields}

    def select(self, releases, tag="v1.10.0", identity=10, resolved=None):
        return publication.select_publication(
            tag, identity, self.revision, releases,
            lambda candidate: {"sha": resolved or self.revision})

    def test_selects_highest_numeric_stable_desktop_without_latest_or_date_order(self):
        releases = [self.release("v1.9.9", 9, published_at="2027-01-01T00:00:00Z"),
                    self.release("plugins/eda/v99.0.0", 99),
                    self.release("v2.0.0-rc.1", 12),
                    self.release("v8.0.0", 13, prerelease=True),
                    self.release("v9.0.0", 14, draft=True), self.release("v1.10.0")]
        self.assertTrue(self.select(releases)["publish"])
        self.assertFalse(self.select(releases, "v1.9.9", 9)["publish"])

    def test_recheck_rejects_build_superseded_during_qualification(self):
        releases = [self.release("v1.10.0")]
        self.assertTrue(self.select(releases)["publish"])
        releases.append(self.release("v1.10.1", 11))
        result = self.select(releases)
        self.assertFalse(result["publish"])
        self.assertIn("v1.10.1", result["reason"])

    def test_prerelease_plugin_or_unpublished_event_does_not_deploy(self):
        releases = [self.release("v1.10.0")]
        for tag in ("v1.11.0-rc.1", "plugins/nsp/v1.0.0", "v1.11.0", "latest"):
            with self.subTest(tag=tag):
                self.assertFalse(self.select(releases, tag)["publish"])
        self.assertFalse(self.select([])["publish"])

    def test_mutable_release_mismatched_id_or_wrong_commit_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.select([self.release("v1.10.0", immutable=False)])
        with self.assertRaisesRegex(ValueError, "identity"):
            self.select([self.release("v1.10.0", 11)])
        with self.assertRaisesRegex(ValueError, "commit"):
            self.select([self.release("v1.10.0")], resolved="b" * 40)

    def test_malformed_api_data_is_not_treated_as_an_empty_catalog(self):
        for value in ({"message": "API failure"}, [None], [{"tag_name": "v1.10.0"}]):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.select(value)
        duplicate = self.release("v1.10.0")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.select([duplicate, deepcopy(duplicate)])

    def test_guard_checks_paginated_catalog_and_does_not_authorize_after_api_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = root / "event.json"
            output = root / "output"
            event_release = self.release("v1.10.0")
            event_release.pop("immutable")  # This field belongs to the REST API, not the webhook contract.
            event.write_text(json.dumps({"action": "published", "release": event_release}))
            environment = {"GITHUB_EVENT_NAME": "release", "GITHUB_EVENT_PATH": str(event),
                           "GITHUB_REF": "refs/tags/v1.10.0", "GITHUB_SHA": self.revision,
                           "GITHUB_REPOSITORY": "owner/repo", "GITHUB_OUTPUT": str(output)}
            pages = [[self.release("v1.9.0", 9)], [self.release("v1.10.0")]]
            with patch.object(publication.subprocess, "check_output", side_effect=[
                    self.revision, json.dumps(pages), json.dumps({"sha": self.revision})]) as command:
                self.assertTrue(publication.guard_publication(root, environment)["publish"])
                self.assertIn("--paginate", command.call_args_list[1].args[0])
            self.assertEqual(output.read_text(), "publish=true\n")
            output.unlink()
            pages.append([self.release("v1.10.1", 11)])
            with patch.object(publication.subprocess, "check_output", side_effect=[
                    self.revision, json.dumps(pages)]):
                self.assertFalse(publication.guard_publication(root, environment)["publish"])
            self.assertEqual(output.read_text(), "publish=false\n")
            output.unlink()
            with patch.object(publication.subprocess, "check_output", side_effect=[
                    self.revision, subprocess.CalledProcessError(1, ["gh", "api"])]):
                with self.assertRaises(subprocess.CalledProcessError):
                    publication.guard_publication(root, environment)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
