"""Release history stays complete while sidebar versions follow the publication boundary."""

import json
from pathlib import Path
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs import publication
from docs.releases import configure_release_navigation, release_navigation


class ReleaseNavigationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.directory = self.root / "website/docs/releases"
        self.directory.mkdir(parents=True)
        (self.directory / "index.md").write_text(
            "# Releases\n\n<!-- release-history -->\n")
        self.configuration = self.root / "website/zensical.toml"
        self.baseline("v1.0.0")

    def baseline(self, tag):
        self.configuration.write_text(
            '[project]\nnav = [{ "Home" = "index.md" }, '
            '{ "Releases" = "releases/index.md" }]\n'
            f'[project.extra]\ndesktop_release = "{tag}"\n')

    def notes(self, tag, metadata=""):
        version = publication.release_version(tag, historical=True)
        page = self.directory / f"{tag}.md"
        page.write_text(f"---\nrelease_tag: {tag}\nrelease_version: {version}\n"
                        f"{metadata}---\n\n# StreamSkope {tag}\n\nArchived release body.\n")
        return page

    def preview(self):
        page = self.directory / "unreleased.md"
        page.write_text("---\nunreleased: true\n---\n\n# Unreleased changes\n")
        return page

    def rendered(self, published=False):
        return tomllib.loads(configure_release_navigation(
            self.configuration.read_text(), self.root, published))["project"]

    def publication_event(self):
        (self.root / "package.json").write_text(json.dumps({"version": "0.9.0"}))
        qualification = self.root / "website/docs/guide/qualification.md"
        qualification.parent.mkdir(exist_ok=True)
        qualification.write_text("# Qualification\n\n## Historical qualification: v0.8.0\n")
        event = self.root / "event.json"
        body = "# StreamSkope v0.9.0\n\nExact reviewed publication notes.\n"
        event.write_text(json.dumps({"action": "published", "release": {
            "tag_name": "v0.9.0", "draft": False, "prerelease": False, "body": body,
            "published_at": "2026-10-06T08:15:00Z",
        }}))
        return {"GITHUB_EVENT_NAME": "release", "GITHUB_EVENT_PATH": str(event),
                "GITHUB_REF_NAME": "v0.9.0", "GITHUB_REF": "refs/tags/v0.9.0",
                "GITHUB_SHA": "a" * 40, "STREAMSKOPE_DOCS_PUBLISH": "1"}, body

    def test_five_recent_stable_versions_and_full_history_use_numeric_semver(self):
        tags = ["v0.9.0", "v0.4.0", "v0.8.0", "v0.10.0", "v0.7.0", "v0.6.0", "v0.5.0"]
        for tag in tags:
            self.notes(tag)
        project = self.rendered()
        fields = project["extra"]["release_navigation"]
        self.assertEqual(fields["recent_tags"],
                         ["v0.10.0", "v0.9.0", "v0.8.0", "v0.7.0", "v0.6.0"])
        self.assertEqual(fields["all_tags"],
                         ["v0.10.0", "v0.9.0", "v0.8.0", "v0.7.0", "v0.6.0", "v0.5.0", "v0.4.0"])
        branch = project["nav"][1]["Releases"]
        self.assertEqual(branch[-1], {"See all releases": "releases/index.md"})
        self.assertEqual([next(iter(item)) for item in branch[:-1]], fields["all_tags"])
        # An older page remains a Releases child for its breadcrumb ancestors.
        self.assertIn({"v0.4.0": "releases/v0.4.0.md"}, branch)
        self.assertNotIn("releases/v0.4.0.md", fields["recent_paths"])
        self.assertEqual([release["tag"] for release in fields["releases"]], fields["all_tags"])
        self.assertEqual(project["nav"][0], {"Home": "index.md"})

    def test_legacy_and_prerelease_notes_remain_in_history_without_recent_eligibility(self):
        for tag in ("v0.9.0", "v0.10.0-rc.2", "v0.10.0-rc.10", "v0.10.0-beta",
                    "v0.10.0", "v0.1.0+build.1"):
            self.notes(tag)
        fields = release_navigation(self.root)
        self.assertEqual(fields["recent_tags"], ["v0.10.0", "v0.9.0"])
        self.assertEqual(fields["all_tags"], ["v0.10.0", "v0.10.0-rc.10", "v0.10.0-rc.2",
                                              "v0.10.0-beta", "v0.9.0", "v0.1.0+build.1"])
        self.assertIn("releases/v0.1.0+build.1.md", fields["all_paths"])

    def test_prerelease_numeric_identifiers_have_semver_precedence(self):
        for tag in ("v0.10.0-alpha", "v0.10.0-1", "v0.10.0-1.9", "v0.10.0-1.10"):
            self.notes(tag)
        self.assertEqual(release_navigation(self.root)["all_tags"],
                         ["v0.10.0-alpha", "v0.10.0-1.10", "v0.10.0-1.9", "v0.10.0-1"])

    def test_unreleased_is_preview_only_and_overview_is_a_single_final_child(self):
        self.notes("v0.8.0")
        preview = self.preview()
        development = self.rendered()
        public = self.rendered(published=True)
        self.assertEqual(development["nav"][1]["Releases"][0],
                         {"Unreleased": "releases/unreleased.md"})
        self.assertEqual(development["extra"]["release_navigation"]["preview_path"],
                         "releases/unreleased.md")
        self.assertEqual(public["extra"]["release_navigation"]["preview_path"], "")
        self.assertNotIn("unreleased.md", json.dumps(public))
        for project in (development, public):
            branch = project["nav"][1]["Releases"]
            self.assertEqual(sum(item == {"See all releases": "releases/index.md"}
                                 for item in branch), 1)
            self.assertEqual(branch[-1], {"See all releases": "releases/index.md"})
        self.assertTrue(preview.exists())
        preview.unlink()
        self.assertEqual(release_navigation(self.root)["preview_path"], "")

    def test_stamped_candidate_is_pending_until_documented_publication_advances(self):
        self.baseline("v0.8.0")
        self.notes("v0.8.0")
        self.notes("v0.9.0")
        environment, body = self.publication_event()
        fields = release_navigation(self.root)
        self.assertEqual(fields["recent_tags"], ["v0.8.0"])
        self.assertEqual(fields["pending_tags"], ["v0.9.0"])
        self.assertEqual(fields["pending_paths"], ["releases/v0.9.0.md"])
        self.assertEqual([release["tag"] for release in fields["releases"]], ["v0.9.0", "v0.8.0"])
        with self.assertRaisesRegex(ValueError, "pending release notes: releases/v0.9.0.md"):
            self.rendered(published=True)
        with patch.object(publication.subprocess, "check_output", return_value="a" * 40):
            publication.prepare_publication(self.root, environment)
        fields = self.rendered(published=True)["extra"]["release_navigation"]
        self.assertEqual(fields["recent_tags"], ["v0.9.0", "v0.8.0"])
        self.assertEqual(fields["pending_tags"], [])
        self.assertEqual(fields["pending_paths"], [])
        self.assertEqual(fields["all_paths"], ["releases/v0.9.0.md", "releases/v0.8.0.md"])
        self.assertFalse(any(release["pending"] for release in fields["releases"]))
        self.assertEqual((self.directory / "v0.9.0.md").read_text().split("---\n\n", 1)[1], body)

    def test_explicit_pending_status_keeps_reviewed_notes_out_of_recent_list(self):
        self.notes("v0.8.0")
        page = self.notes("v0.9.0", "release_status: pending\n")
        fields = release_navigation(self.root)
        self.assertEqual(fields["all_tags"], ["v0.9.0", "v0.8.0"])
        self.assertEqual(fields["recent_tags"], ["v0.8.0"])
        self.assertEqual(fields["pending_tags"], ["v0.9.0"])
        with self.assertRaisesRegex(ValueError, "pending release notes: releases/v0.9.0.md"):
            self.rendered(published=True)
        page.write_text(page.read_text().replace("release_status: pending\n", ""))
        self.assertEqual(release_navigation(self.root)["recent_tags"], ["v0.9.0", "v0.8.0"])

    def test_publication_rejects_other_pending_pages_remaining_after_event_preparation(self):
        self.baseline("v0.8.0")
        self.notes("v0.8.0")
        self.notes("v0.10.0")
        environment, _ = self.publication_event()
        with patch.object(publication.subprocess, "check_output", return_value="a" * 40):
            publication.prepare_publication(self.root, environment)
        self.assertEqual(release_navigation(self.root)["pending_paths"], ["releases/v0.10.0.md"])
        with self.assertRaisesRegex(ValueError, "pending release notes: releases/v0.10.0.md"):
            self.rendered(published=True)

    def test_mismatched_or_duplicate_release_identity_metadata_fails(self):
        page = self.notes("v0.8.0")
        original = page.read_text()
        invalid = [original.replace("release_tag: v0.8.0", "release_tag: v0.9.0"),
                   original.replace("release_version: 0.8.0", "release_version: 0.9.0"),
                   original.replace("release_tag: v0.8.0", "release_tag: v0.8.0\nrelease_tag: v0.8.0"),
                   original.replace("release_version: 0.8.0",
                                    "release_version: 0.8.0\nrelease_version: 0.8.0")]
        for content in invalid:
            with self.subTest(content=content):
                page.write_text(content)
                with self.assertRaisesRegex(ValueError, "v0.8.0.md:.*exact tag and application version"):
                    release_navigation(self.root)

    def test_invalid_or_duplicate_status_metadata_fails(self):
        for metadata in ("release_status: draft\n", "release_status: \n",
                         "release_status: pending\nrelease_status: pending\n"):
            with self.subTest(metadata=metadata):
                self.notes("v0.8.0", metadata)
                with self.assertRaisesRegex(ValueError, "release_status"):
                    release_navigation(self.root)

    def test_overview_uses_archived_date_and_summary_without_inventing_metadata(self):
        self.notes("v0.9.0", 'release_date: "2026-10-05"\n'
                   'release_summary: "Native subscriptions and reviewed package installation"\n')
        self.notes("v0.8.0")
        entries = self.rendered()["extra"]["release_navigation"]["releases"]
        self.assertEqual(entries[0]["date"], "2026-10-05")
        self.assertEqual(entries[0]["summary"], "Native subscriptions and reviewed package installation")
        self.assertEqual(entries[1]["date"], "")
        self.assertEqual(entries[1]["summary"], "Archived release body.")

    def test_invalid_or_duplicate_archived_dates_fail(self):
        for metadata in ('release_date: "2026-02-30"\n', 'release_date: "2026-2-03"\n',
                         'release_date: "2026-10-05"\nrelease_date: "2026-10-05"\n'):
            with self.subTest(metadata=metadata):
                self.notes("v0.8.0", metadata)
                with self.assertRaisesRegex(ValueError, "release_date"):
                    release_navigation(self.root)

    def test_nonrelease_filenames_and_missing_overview_fail(self):
        page = self.directory / "v0.08.0.md"
        page.write_text("# Invalid tag\n")
        with self.assertRaisesRegex(ValueError, "filename needs an exact desktop tag"):
            release_navigation(self.root)
        page.unlink()
        (self.directory / "index.md").unlink()
        with self.assertRaisesRegex(ValueError, "overview page"):
            release_navigation(self.root)

    def test_missing_or_duplicate_configuration_placeholder_fails(self):
        for source in ('[project]\nnav = []\n',
                       self.configuration.read_text().replace(
                           '{ "Releases" = "releases/index.md" }',
                           '{ "Releases" = "releases/index.md" }, { "Releases" = "releases/index.md" }')):
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, "one Releases overview placeholder"):
                configure_release_navigation(source, self.root)

    def test_missing_or_duplicate_overview_marker_fails(self):
        index = self.directory / "index.md"
        for content in ("# Releases\n", "<!-- release-history -->\n<!-- release-history -->\n"):
            with self.subTest(content=content):
                index.write_text(content)
                with self.assertRaisesRegex(ValueError, "exactly one release-history marker"):
                    release_navigation(self.root)

    def test_publication_materialization_adds_current_page_and_removes_preview_link(self):
        self.baseline("v0.8.0")
        self.notes("v0.8.0")
        self.preview()
        environment, body = self.publication_event()
        self.assertFalse((self.directory / "v0.9.0.md").exists())
        with patch.object(publication.subprocess, "check_output", return_value="a" * 40):
            publication.prepare_publication(self.root, environment)
        project = self.rendered(published=True)
        fields = project["extra"]["release_navigation"]
        self.assertEqual(fields["recent_tags"], ["v0.9.0", "v0.8.0"])
        self.assertEqual(fields["pending_tags"], [])
        self.assertEqual(fields["preview_path"], "")
        self.assertEqual(project["nav"][1]["Releases"][0], {"v0.9.0": "releases/v0.9.0.md"})
        self.assertNotIn("unreleased.md", json.dumps(project))
        self.assertEqual((self.directory / "v0.9.0.md").read_text().split("---\n\n", 1)[1], body)
        self.assertEqual(fields["releases"][0]["tag"], "v0.9.0")
        self.assertEqual(fields["releases"][0]["date"], "2026-10-06")
        self.assertEqual((self.directory / "index.md").read_text(), "# Releases\n\n<!-- release-history -->\n")


if __name__ == "__main__":
    unittest.main()
