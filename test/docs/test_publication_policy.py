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


class ArchivedQualificationTests(unittest.TestCase):
    revision = "b" * 40

    def plan(self):
        tag = "v0.11.0"
        return {"latestStableDesktop": {
            "tag": tag, "sourceSha": self.revision,
            "release": {"tag_name": tag, "draft": False, "prerelease": False,
                        "immutable": True, "assets": []}}}

    def page(self, root):
        page = root / "website/docs/guide/qualification.md"
        page.parent.mkdir(parents=True)
        page.write_text(
            "# Qualification evidence\n\n"
            "<!-- publication-qualification -->\n## Published release: v0.10.0\n\n"
            "A reviewed report records a blocked live NSP test.\n"
            "<!-- /publication-qualification -->\n\n"
            "## Historical qualification: v0.9.2\n\nEarlier retained limits.\n")
        return page

    def test_requires_archive_before_next_release_and_preserves_prior_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = self.page(root)
            before = page.read_text()
            with patch.object(publication.subprocess, "check_output", return_value=self.revision):
                with self.assertRaisesRegex(ValueError, "need archival"):
                    publication.archived_qualification(root, self.plan(), check=True)
                self.assertEqual(page.read_text(), before)
                publication.archived_qualification(root, self.plan())
                updated = page.read_text()
                self.assertIn("## Published release: v0.11.0", updated)
                self.assertIn(f"/commit/{self.revision}", updated)
                self.assertIn("No source-specific qualification report", updated)
                self.assertIn("## Historical qualification: v0.10.0", updated)
                self.assertIn("A reviewed report records a blocked live NSP test.", updated)
                self.assertIn("Earlier retained limits.", updated)
                publication.archived_qualification(root, self.plan(), check=True)
                publication.archived_qualification(root, self.plan())
                self.assertEqual(page.read_text(), updated)

    def test_preserves_reviewed_current_report_and_links_only_recorded_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = self.page(root)
            plan = self.plan()
            asset = {"name": "qualification-v0.11.0.json", "state": "uploaded", "size": 10,
                     "browser_download_url": "https://github.com/asadarafat/streamskope/releases/download/v0.11.0/qualification-v0.11.0.json"}
            plan["latestStableDesktop"]["release"]["assets"] = [asset]
            with patch.object(publication.subprocess, "check_output", return_value=self.revision):
                publication.archived_qualification(root, plan)
                self.assertIn(asset["browser_download_url"], page.read_text())
                self.assertIn("the link alone does not establish", page.read_text())
                page.write_text(page.read_text().replace(
                    "<!-- /publication-qualification -->", "A reviewed failed test remains recorded.\n<!-- /publication-qualification -->"))
                reviewed = page.read_text()
                publication.archived_qualification(root, plan)
                self.assertEqual(page.read_text(), reviewed)

    def test_rejects_mutable_release_or_different_tag_source_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = self.page(root)
            before = page.read_text()
            plan = self.plan()
            plan["latestStableDesktop"]["release"]["immutable"] = False
            with self.assertRaisesRegex(ValueError, "immutable"):
                publication.archived_qualification(root, plan)
            with patch.object(publication.subprocess, "check_output", return_value="c" * 40):
                with self.assertRaisesRegex(ValueError, "source differs"):
                    publication.archived_qualification(root, self.plan())
            self.assertEqual(page.read_text(), before)


if __name__ == "__main__":
    unittest.main()
