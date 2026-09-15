"""Behavioral checks for the public documentation artifact boundary."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("docs", Path(__file__).with_name("docs.py"))
docs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs)


class DocumentationArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "guide").mkdir()
        (self.root / "guide/index.html").write_text('<h1 id="connect">Connect</h1>')

    def test_accepts_relative_and_project_subpath_links(self):
        (self.root / "index.html").write_text(
            '<a href="guide/#connect">Guide</a>'
            '<a href="/streamskope/guide/#connect">Connect</a>'
        )
        docs.inspect_site(self.root, "/streamskope/")

    def test_accepts_only_named_public_intro_exports(self):
        (self.root / "assets").mkdir()
        (self.root / "assets/streamskope-intro-light.mp4").write_bytes(b"public light export")
        (self.root / "assets/streamskope-intro-dark.mp4").write_bytes(b"public dark export")
        (self.root / "index.html").write_text('<video src="assets/streamskope-intro-light.mp4"></video>')
        docs.inspect_site(self.root)
        (self.root / "assets/private.mp4").write_bytes(b"private recording")
        with self.assertRaisesRegex(ValueError, "Unexpected publication file"):
            docs.inspect_site(self.root)

    def test_rejects_missing_page_and_anchor(self):
        for link in ["missing/", "guide/#missing"]:
            with self.subTest(link=link):
                (self.root / "index.html").write_text(f'<a href="{link}">Broken</a>')
                with self.assertRaisesRegex(ValueError, "Broken local link"):
                    docs.inspect_site(self.root, "/streamskope/")

    def test_rejects_private_artifacts_and_symlinks(self):
        (self.root / "index.html").write_text("<h1>Home</h1>")
        for name in ["private.pem", "profile.json", "private.mp4", "private.m4a", "private.mp3"]:
            with self.subTest(name=name):
                artifact = self.root / name
                artifact.write_text("not public")
                with self.assertRaisesRegex(ValueError, "Unexpected publication file"):
                    docs.inspect_site(self.root, "/")
                artifact.unlink()
        link = self.root / "alias.html"
        link.symlink_to(self.root / "index.html")
        with self.assertRaisesRegex(ValueError, "symlink"):
            docs.inspect_site(self.root, "/")

    def test_rejects_traversal_and_machine_paths(self):
        for html in ['<a href="../../outside.html">Outside</a>', '<p>/Users/alice/private</p>']:
            (self.root / "index.html").write_text(html)
            with self.assertRaises(ValueError):
                docs.inspect_site(self.root, "/")


if __name__ == "__main__":
    unittest.main()
