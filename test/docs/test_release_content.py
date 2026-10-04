"""Unreleased component prose must not mislabel the published desktop."""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs.publication import inspect_release_content


class ReleaseContentTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def page(self, path, content):
        file = self.root / "website/docs" / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        return file

    def test_old_plugin_unreleased_banner_fails_even_in_preview(self):
        self.page("plugins/eda.md", "---\nunreleased: true\n---\n# EDA Capture\n")
        for published in (True, False):
            with self.subTest(published=published), self.assertRaisesRegex(ValueError, "ambiguous unreleased"):
                inspect_release_content(self.root, published)

    def test_explicit_plugin_scope_is_valid_without_claiming_publication(self):
        file = self.page("plugins/eda.md", "---\nplugin_scope: eda\n---\n# EDA Capture\n")
        inspect_release_content(self.root, True)
        for scope in ("all", "nsp", "missing"):
            file.write_text(f"---\nplugin_scope: {scope}\n---\n# EDA Capture\n")
            with self.subTest(scope=scope), self.assertRaisesRegex(ValueError, "plugin_scope"):
                inspect_release_content(self.root, True)

    def test_development_notes_cannot_enter_published_site(self):
        file = self.page("releases/unreleased.md", "---\nunreleased: true\n---\n# Unreleased changes\n")
        inspect_release_content(self.root)
        with self.assertRaises(ValueError):
            inspect_release_content(self.root, True)
        file.write_text("# Unreleased changes\n")
        with self.assertRaisesRegex(ValueError, "unreleased notes page"):
            inspect_release_content(self.root, True)

    def test_stale_current_source_heading_fails_but_historical_evidence_survives(self):
        file = self.page("guide/qualification.md", "## Current-source qualification\n")
        with self.assertRaisesRegex(ValueError, "source-bound"):
            inspect_release_content(self.root)
        file.write_text("## Source-bound rehearsal evidence\nExact source candidate abc123.\n"
                        "## Historical qualification: v0.7.0\nSome tests were unexecuted.\n")
        inspect_release_content(self.root, True)


if __name__ == "__main__":
    unittest.main()
